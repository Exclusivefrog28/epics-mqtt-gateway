package org.excf.epicsmqtt.gateway.adapter.ca;

import gov.aps.jca.JCALibrary;
import gov.aps.jca.cas.ServerContext;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_String;
import io.quarkus.logging.Log;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ChannelAccessAdapter extends Adapter {
    private ServerContext caServer;
    private CAClient caClient;

    private Thread caServerThread;

    @Override
    public String getHosted(String channel) throws Exception {
        return ((DBR_String) caClient.get(channel).convert(DBRType.STRING)).getStringValue()[0];
    }

    @Override
    public void putHosted(String channel, String value) throws Exception {
        caClient.put(channel, value);
    }

    @Override
    public String getExternal(String channel) {
        return bridge.get(channel);
    }

    @Override
    public void putExternal(String channel, String value) {
        bridge.put(channel, value);
    }

    @jakarta.annotation.PostConstruct
    public void startup() {
        try {
            System.setProperty("com.cosylab.epics.caj.CAJContext.name_servers", "127.0.0.1:9064");

            caServer = JCALibrary.getInstance().createServerContext(JCALibrary.CHANNEL_ACCESS_SERVER_JAVA, new CAServer(this));
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

    @jakarta.annotation.PreDestroy
    public void cleanup() {
        try {
            if (caServerThread != null){
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
