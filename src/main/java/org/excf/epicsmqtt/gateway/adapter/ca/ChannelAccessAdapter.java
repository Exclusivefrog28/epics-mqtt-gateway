package org.excf.epicsmqtt.gateway.adapter.ca;

import gov.aps.jca.JCALibrary;
import gov.aps.jca.cas.ServerContext;
import gov.aps.jca.dbr.*;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.model.PV;
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
    public Uni<PVValue> getHosted(String channel) {
        return caClient.get(channel).onItem().transform(this::convertDBRToPVValue);
    }

    @Override
    public Multi<PV> monitorHosted(String channel) {
        return caClient.attachMonitor(channel)
                .onItem().transform(this::convertDBRToPVValue)
                .onItem().transform(pvValue -> new PV(channel, pvValue, true));
    }

    @Override
    public Uni<Void> putHosted(PV pv) {
        return caClient.put(pv.pvName, pv.pvValue.value);
    }

    public Multi<PV> addExternalMonitor(String channel) {
        return bridge.monitorExternal(channel);
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
            pvValue.setStatus(sts.getStatus());
            pvValue.setSeverity(sts.getSeverity());
        }

        // Extract Time
        if (dbr instanceof TIME t) {
            TimeStamp ts = t.getTimeStamp();
            if (ts != null) {
                pvValue.timestamp = Instant.ofEpochSecond(ts.secPastEpoch(), ts.nsec());
            }
        }

        if (dbr instanceof LABELS) {
            pvValue.metadata.labels = ((LABELS) dbr).getLabels();
        }

        return pvValue;
    }

    public void startup(@Observes StartupEvent ev) {
        try {
            System.setProperty("com.cosylab.epics.caj.CAJContext.addr_list", clientAddrList);
            System.setProperty("com.cosylab.epics.caj.CAJContext.auto_addr_list", clientAutoAddrList);

            caServer = JCALibrary.getInstance().createServerContext(JCALibrary.CHANNEL_ACCESS_SERVER_JAVA,
                    new CAServer(this));
            caClient = new CAClient(JCALibrary.getInstance().createContext(JCALibrary.CHANNEL_ACCESS_JAVA),
                    this);

            caServerThread = new Thread(() -> {
                try {
                    caServer.run(0);
                } catch (Throwable th) {
                    Log.error("Could not start ChannelAccess server", th);
                }
            });
            caServerThread.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start ChannelAccessAdapter", e);
        }
    }

    public void cleanup(@Observes ShutdownEvent ev) {
        try {
            if (caServer != null)
                caServer.shutdown();
            if (caServerThread != null) {
                caServerThread.join();
            }
            if (caServer != null)
                caServer.destroy();
        } catch (Exception e) {
            Log.error("CA server didn't shut down gracefully", e);
        }
        try {
            if (caClient != null) caClient.shutDown();
        } catch (Exception e) {
            Log.error("CA client didn't shut down gracefully", e);
        }
    }
}
