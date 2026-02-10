package org.excf.epicsmqtt.gateway.adapter.ca;

import com.cosylab.epics.caj.CAJContext;
import gov.aps.jca.*;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import io.quarkus.logging.Log;

import java.util.concurrent.ConcurrentHashMap;

public class CAClient {
    Context context;

    ConcurrentHashMap<String, Channel> openChannels;
    ConcurrentHashMap<String, Monitor> openMonitors;

    private final ChannelAccessAdapter adapter;

    public CAClient(Context context, ChannelAccessAdapter adapter) {
        this.context = context;
        this.adapter = adapter;
        openChannels = new ConcurrentHashMap<>();
        openMonitors = new ConcurrentHashMap<>();
    }

    public synchronized DBR get(String channelName) throws CAException, TimeoutException {
        Channel channel = accessOrOpenChannel(channelName);
        context.pendIO(5.0);

        DBR dbr = channel.get(DBRType.forValue(channel.getFieldType().getValue() + 28), channel.getElementCount());
        context.pendIO(5.0);

        context.flushIO();
        tryCloseChannel(channel);
        return dbr;
    }

    public synchronized DBR get(String channelName, DBRType type) throws CAException, TimeoutException {
        Channel channel = accessOrOpenChannel(channelName);
        context.pendIO(5.0);

        DBR dbr = channel.get(type, channel.getElementCount());
        context.pendIO(5.0);

        context.flushIO();
        tryCloseChannel(channel);
        return dbr;
    }

    public synchronized void attachMonitor(String channelName) throws CAException, TimeoutException {
        Channel channel = accessOrOpenChannel(channelName);
        context.pendIO(5.0);
        channel.printInfo();

        openMonitors.computeIfAbsent(channelName, c -> {
            try {
                return channel.addMonitor(DBRType.forValue(channel.getFieldType().getValue() + 28), channel.getElementCount(), Monitor.VALUE,
                        ev -> adapter.put(channelName, adapter.convertDBRToPVValue(ev.getDBR())));
            } catch (CAException e) {
                Log.warn("Couldn't attach monitor to %s".formatted(channelName));
                throw new RuntimeException(e);
            }
        });

        context.pendIO(5.0);
        context.flushIO();
    }

    public synchronized void detachMonitor(String channelName) throws CAException {
        if (openMonitors.containsKey(channelName)) {
            openMonitors.get(channelName).clear();
            openMonitors.remove(channelName);
        }

        tryCloseChannel(channelName);
    }

    public synchronized void put(String channelName, Object value) throws CAException, TimeoutException {
        Channel channel = accessOrOpenChannel(channelName);
        context.pendIO(5.0);

        switch (value) {
            case int[] ig -> channel.put(ig);
            case double[] du -> channel.put(du);
            case byte[] by -> channel.put(by);
            case short[] sh -> channel.put(sh);
            case float[] fl -> channel.put(fl);
            case String[] st -> channel.put(st);
            default -> throw new IllegalStateException("Unexpected value: " + value);
        }
        context.pendIO(5.0);

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

    private void tryCloseChannel(String channelName) throws CAException {
        if (openChannels.containsKey(channelName))
            tryCloseChannel(openChannels.get(channelName));
    }

    private void tryCloseChannel(Channel channel) throws CAException {
        if (!openMonitors.containsKey(channel.getName())) {
            channel.destroy();
            openChannels.remove(channel.getName());
        }
    }

}
