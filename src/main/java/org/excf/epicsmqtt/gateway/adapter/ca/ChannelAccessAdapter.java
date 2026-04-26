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
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.model.PVValue;

import java.time.Instant;

@ApplicationScoped
@Named("ca")
public class ChannelAccessAdapter extends Adapter {

    static long TS_EPOCH_SEC_PAST_1970=7305*86400;

    @Override
    public String protocol() {
        return "ca";
    }

    private ServerContext caServer;

    @ConfigProperty(name = "ca.client.auto.addr.list")
    String clientAutoAddrList;
    @ConfigProperty(name = "ca.client.addr.list")
    String clientAddrList;

    @ConfigProperty(name = "ca.server.server.port", defaultValue = "5064")
    String serverPort;
    @ConfigProperty(name = "ca.server.repeater.port", defaultValue = "5065")
    String serverRepeaterPort;

    @ConfigProperty(name = "ca.maxarraybytes", defaultValue = "16384")
    String maxArrayBytes;

    private CAClient caClient;

    private Thread caServerThread;

    @Override
    public Uni<PVValue> getHosted(String name) {
        return caClient.get(name).onItem().transform(this::convertDBRToPVValue);
    }

    @Override
    public Multi<PVValue> monitorHosted(String name) {
        return caClient.attachMonitor(name)
                .onItem().transform(this::convertDBRToPVValue);
    }

    @Override
    public Uni<Void> putHosted(String name, PVValue pvValue) {
        return caClient.put(name, pvValue.value);
    }

    PVValue convertDBRToPVValue(DBR dbr) {
        PVValue pvValue = new PVValue();

        pvValue.value = dbr.getValue();

        DBRType complexType = dbr.getType();
        if (complexType.isSTRING())
            pvValue.setDBRType(DBRType.STRING);
        else if (complexType.isSHORT())
            pvValue.setDBRType(DBRType.SHORT);
        else if (complexType.isFLOAT())
            pvValue.setDBRType(DBRType.FLOAT);
        else if (complexType.isENUM())
            pvValue.setDBRType(DBRType.ENUM);
        else if (complexType.isBYTE())
            pvValue.setDBRType(DBRType.BYTE);
        else if (complexType.isINT())
            pvValue.setDBRType(DBRType.INT);
        else if (complexType.isDOUBLE())
            pvValue.setDBRType(DBRType.DOUBLE);

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
            if (ts != null)
                pvValue.timestamp = Instant.ofEpochSecond(ts.secPastEpoch() + TS_EPOCH_SEC_PAST_1970, ts.nsec());
        }

        if (dbr instanceof LABELS) {
            pvValue.metadata.labels = ((LABELS) dbr).getLabels();
        }

        if (pvValue.timestamp == null)
            pvValue.timestamp = Instant.now();

        return pvValue;
    }

    void startup(@Observes StartupEvent ev) {
        try {
            System.setProperty("com.cosylab.epics.caj.CAJContext.addr_list", clientAddrList);
            System.setProperty("com.cosylab.epics.caj.CAJContext.auto_addr_list", clientAutoAddrList);

            System.setProperty("com.cosylab.epics.caj.cas.CAJServerContext.server_port", serverPort);
            System.setProperty("com.cosylab.epics.caj.cas.CAJServerContext.repeater_port", serverRepeaterPort);

            System.setProperty("com.cosylab.epics.caj.cas.CAJServerContext,max_array_bytes", maxArrayBytes);

            caServer = JCALibrary.getInstance().createServerContext(JCALibrary.CHANNEL_ACCESS_SERVER_JAVA,
                    new CAServer(this));
            caClient = new CAClient(JCALibrary.getInstance().createContext(JCALibrary.CHANNEL_ACCESS_JAVA));

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

    void cleanup(@Observes ShutdownEvent ev) {
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
