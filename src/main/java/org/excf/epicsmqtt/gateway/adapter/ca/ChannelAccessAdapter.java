package org.excf.epicsmqtt.gateway.adapter.ca;

import gov.aps.jca.JCALibrary;
import gov.aps.jca.cas.ServerContext;
import gov.aps.jca.dbr.*;
import io.quarkus.logging.Log;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.model.PVValue;

import java.time.Instant;

@ApplicationScoped
public class ChannelAccessAdapter extends Adapter {

    private ServerContext caServer;

    @ConfigProperty(name = "ca.client.auto.addr.list")
    String clientAutoAddrList;
    @ConfigProperty(name = "ca.client.addr.list")
    String clientAddrList;

    private CAClient caClient;

    private Thread caServerThread;

    @Override
    public PVValue getHosted(String channel) throws Exception {
        DBR dbr = caClient.get(channel);
        return convertDBRToPVValue(dbr);
    }

    @Override
    public void putHosted(String channel, PVValue value) throws Exception {
        caClient.put(channel, value.value);
    }

    PVValue convertDBRToPVValue(DBR dbr) {
        PVValue pvValue = new PVValue();

        pvValue.value = dbr.getValue();
        pvValue.setDBRType(dbr.getType());

        // Extract Metadata
        if (dbr instanceof GR gr) {
            pvValue.metadata.units = gr.getUnits();
            pvValue.metadata.upperDisplayLimit = gr.getUpperDispLimit();
            pvValue.metadata.lowerDisplayLimit = gr.getLowerDispLimit();
            pvValue.metadata.upperAlarmLimit = gr.getUpperAlarmLimit();
            pvValue.metadata.upperWarningLimit = gr.getUpperWarningLimit();
            pvValue.metadata.lowerWarningLimit = gr.getLowerWarningLimit();
            pvValue.metadata.lowerAlarmLimit = gr.getLowerAlarmLimit();
        }

        // Extract precision
        if (dbr instanceof PRECISION pr) {
            pvValue.metadata.precision = (int) pr.getPrecision();
        }

        // Extract control
        if (dbr instanceof CTRL ctrl) {
            pvValue.metadata.upperControlLimit = ctrl.getUpperCtrlLimit();
            pvValue.metadata.lowerControlLimit = ctrl.getLowerCtrlLimit();
        }

        // Extract Status
        if (dbr instanceof STS sts) {
            pvValue.status = sts.getStatus();
            pvValue.severity = sts.getSeverity();
        }

        // Extract Time
        if (dbr instanceof TIME t) {
            TimeStamp ts = t.getTimeStamp();
            if (ts != null) {
                pvValue.timestamp = Instant.ofEpochSecond(ts.secPastEpoch(), ts.nsec());
            }
        }

        return pvValue;
    }

    @PostConstruct
    public void startup() {
        try {
            System.setProperty("com.cosylab.epics.caj.CAJContext.addr_list", clientAddrList);
            System.setProperty("com.cosylab.epics.caj.CAJContext.auto_addr_list", clientAutoAddrList);

            caServer = JCALibrary.getInstance().createServerContext(JCALibrary.CHANNEL_ACCESS_SERVER_JAVA,
                    new CAServer(this));
            caClient = new CAClient(JCALibrary.getInstance().createContext(JCALibrary.CHANNEL_ACCESS_JAVA));

            caServerThread = new Thread(() -> {
                try {
                    caServer.run(0);
                } catch (Throwable th) {
                    Log.error("ChannelAccess server didn't shut down gracefully", th);
                }
            });
            caServerThread.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start ChannelAccessAdapter", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        try {
            if (caServerThread != null) {
                caServerThread.interrupt();
                caServerThread.join();
            }
            if (caServer != null)
                caServer.destroy();
            if (caClient != null && caClient.context != null)
                caClient.context.destroy();
        } catch (Exception e) {
            Log.error("ChannelAccessAdatper didn't shut down gracefully", e);
        }
    }
}
