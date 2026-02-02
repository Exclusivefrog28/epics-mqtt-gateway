package org.excf.epicsmqtt.gateway.adapter.ca;

import gov.aps.jca.CAException;
import gov.aps.jca.Channel;
import gov.aps.jca.Context;
import gov.aps.jca.TimeoutException;
import gov.aps.jca.dbr.DBR;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CAClient {
    Context context;

    ConcurrentHashMap<String, Channel> openChannels;

    public CAClient(Context context) {
        this.context = context;
        openChannels = new ConcurrentHashMap<>();
    }

    public synchronized DBR get(String channelName) throws CAException, TimeoutException {
        Channel channel = accessOrOpenChannel(channelName);

        context.pendIO(5.0);
        DBR dbr = channel.get();
        context.pendIO(5.0);

        context.flushIO();
        tryCloseChannel(channel);
        return dbr;
    }

    public synchronized void put(String channelName, String value) throws CAException, TimeoutException {
        Channel channel = accessOrOpenChannel(channelName);

        context.pendIO(5.0);
        channel.put(value);
        context.flushIO();

        tryCloseChannel(channel);
    }

    private Channel accessOrOpenChannel(String channelName) {
        return openChannels.computeIfAbsent(channelName, name -> {
            try {
                return context.createChannel(name);
            } catch (CAException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void tryCloseChannel(Channel channel) throws CAException {
        channel.destroy();
        openChannels.remove(channel.getName());
    }


}
