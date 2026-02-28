package org.excf.epicsmqtt.gateway.adapter.ca;

import gov.aps.jca.*;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.event.ConnectionEvent;
import gov.aps.jca.event.ConnectionListener;
import gov.aps.jca.event.PutListener;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.excf.epicsmqtt.gateway.model.PV;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class CAClient {
    Context context;

    ConcurrentHashMap<String, Uni<Channel>> openChannels;
    ConcurrentHashMap<String, Monitor> openMonitors;

    private final ChannelAccessAdapter adapter;

    public CAClient(Context context, ChannelAccessAdapter adapter) {
        this.context = context;
        this.adapter = adapter;
        openChannels = new ConcurrentHashMap<>();
        openMonitors = new ConcurrentHashMap<>();
    }

    public Uni<DBR> get(String channelName) {
        return accessOrOpenChannel(channelName)
                .onItem().transformToUni(channel ->
                        Uni.createFrom().emitter(emitter -> {
                            try {
                                DBRType type = DBRType.forValue(channel.getFieldType().getValue() + 28);

                                channel.get(type, channel.getElementCount(), ev -> {
                                    if (ev.getStatus().isSuccessful()) emitter.complete(ev.getDBR());
                                    else emitter.fail(new IOException("CA Get failed: " + ev.getStatus()));
                                });

                                context.flushIO();
                            } catch (Exception e) {
                                emitter.fail(e);
                            }
                        })
                );
    }

    public Uni<DBR> get(String channelName, DBRType type) {
        return accessOrOpenChannel(channelName)
                .onItem().transformToUni(channel ->
                        Uni.createFrom().emitter(emitter -> {
                            try {
                                channel.get(type, channel.getElementCount(), ev -> {
                                    if (ev.getStatus().isSuccessful()) emitter.complete(ev.getDBR());
                                    else emitter.fail(new IOException("CA Get failed: " + ev.getStatus()));
                                });

                                context.flushIO();
                            } catch (Exception e) {
                                emitter.fail(e);
                            }
                        })
                );
    }

    public Uni<Void> attachMonitor(String channelName) {
        return accessOrOpenChannel(channelName)
                .onItem().transformToUni(channel ->
                        Uni.createFrom().emitter(emitter -> {
                            if (openMonitors.containsKey(channelName)) {
                                emitter.complete(null);
                                return;
                            }

                            try {
                                DBRType type = DBRType.forValue(channel.getFieldType().getValue() + 28);
                                Monitor monitor = channel.addMonitor(type, channel.getElementCount(), Monitor.VALUE,

                                        ev -> adapter.update(
                                                        new PV(channelName, adapter.convertDBRToPVValue(ev.getDBR()))
                                                )
                                                .subscribe().with(
                                                        unused -> {
                                                        },
                                                        failure -> Log.warnf("Failed to send monitored value on channel %s", channelName, failure)
                                                )
                                );
                                openMonitors.put(channelName, monitor);

                                context.flushIO();
                                emitter.complete(null);
                            } catch (Exception e) {
                                emitter.fail(e);
                            }
                        })
                );
    }

    public void detachMonitor(String channelName) {
        Monitor monitor = openMonitors.remove(channelName);
        if (monitor != null) {
            try {
                monitor.clear();
                context.pendIO(5000);
                context.flushIO();
            } catch (Exception e) {
                Log.errorf(e, "Failed to clear monitor for %s", channelName);
            }
        }
    }

    public Uni<Void> put(String channelName, Object value) {
        return accessOrOpenChannel(channelName)
                .onItem().transformToUni(channel ->
                        Uni.createFrom().emitter(emitter -> {
                            try {
                                PutListener listener = ev -> Infrastructure.getDefaultExecutor().execute(() -> {
                                    if (ev.getStatus().isSuccessful()) {
                                        emitter.complete(null);
                                    } else {
                                        emitter.fail(new IOException("CA Put failed: " + ev.getStatus()));
                                    }
                                });

                                switch (value) {
                                    case int[] ig -> channel.put(ig, listener);
                                    case double[] du -> channel.put(du, listener);
                                    case byte[] by -> channel.put(by, listener);
                                    case short[] sh -> channel.put(sh, listener);
                                    case float[] fl -> channel.put(fl, listener);
                                    case String[] st -> channel.put(st, listener);
                                    default -> emitter.fail(new IllegalStateException("Unexpected value: " + value));
                                }

                                context.flushIO();
                            } catch (Exception e) {
                                emitter.fail(e);
                            }
                        })
                );
    }

    private Uni<Channel> accessOrOpenChannel(String channelName) {
        return openChannels.computeIfAbsent(channelName, name ->
                Uni.createFrom().<Channel>emitter(emitter ->
                        {
                            try {
                                Channel channel = context.createChannel(name);

                                if (channel.getConnectionState() == Channel.CONNECTED) {
                                    emitter.complete(channel);
                                    return;
                                }

                                channel.addConnectionListener(new ConnectionListener() {
                                    @Override
                                    public void connectionChanged(ConnectionEvent ev) {
                                        if (ev.isConnected()) {
                                            try {
                                                channel.removeConnectionListener(this);
                                                emitter.complete(channel);
                                            } catch (CAException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }
                                    }
                                });

                                context.flushIO();
                            } catch (CAException e) {
                                emitter.fail(e);
                                openChannels.remove(channelName);
                            }
                        })
                        .memoize()
                        .indefinitely()
        );
    }

    public void shutDown() throws CAException {
        for (String channel : openMonitors.keySet()) {
            detachMonitor(channel);
        }

        openMonitors.clear();
        openChannels.clear();

        context.destroy();
    }
}
