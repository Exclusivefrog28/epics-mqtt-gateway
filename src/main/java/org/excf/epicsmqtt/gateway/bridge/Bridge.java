package org.excf.epicsmqtt.gateway.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.config.ExternalChannel;
import org.excf.epicsmqtt.gateway.config.HostedChannel;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.excf.epicsmqtt.gateway.mqtt.MqttService;
import org.excf.epicsmqtt.gateway.mqtt.SignedMessage;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@ApplicationScoped
public class Bridge {

    @Inject
    ObjectMapper mapper;

    @Inject
    PVCache pvCache;

    protected Map<String, Adapter> adapters = new HashMap<>();
    protected Map<String, String> topicMap = new ConcurrentHashMap<>();
    protected Map<String, Cancellable> monitors = new HashMap<>();

    @Inject
    Instance<Adapter> adapterInstances;

    @Inject
    MqttService mqttService;

    public void registerHosted(HostedChannel channel) {
        topicMap.put(channel.pvName, channel.mqttTopic);
        for (Adapter a : adapterInstances) {
            if (a instanceof ChannelAccessAdapter) {
                adapters.put(channel.pvName, a);
                if (channel.monitor)
                    addMonitor(channel, a);
                else
                    mqttService.subscribe(channel.mqttTopic + "/MONITOR")
                            .onItem().transformToUniAndConcatenate(request ->
                                    parseBooleanMessage(request)
                                            .onItem().invoke(shouldMonitor -> {
                                                if (shouldMonitor) addMonitor(channel, a);
                                            })
                            ).subscribe().with(unused -> {
                            }, e -> Log.error("MQTT stream error", e));

                mqttService.subscribe(channel.mqttTopic + "/GET")
                        .onItem().transformToUniAndConcatenate(request ->
                                a.getHosted(channel.pvName)
                                        .onItem().transform(pvValue -> new PV(channel.pvName, pvValue))
                                        .chain(this::update)
                                        .onFailure().recoverWithNull()
                        ).subscribe().with(unused -> {
                        }, e -> Log.error("MQTT stream error", e));
            }
        }

        mqttService.subscribe(channel.mqttTopic + "/PUT")
                .onItem().transformToUniAndConcatenate(message ->
                        parseMessage(message)
                                .chain(this::handledHostedPut)
                                .onFailure().recoverWithNull()
                )
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    public void removeHosted(HostedChannel channel) {
        if (monitors.containsKey(channel.mqttTopic)) {
            monitors.get(channel.mqttTopic).cancel();
            monitors.remove(channel.mqttTopic);
        }
        adapters.remove(channel.pvName);
        mqttService.unsubscribe(channel.mqttTopic);
    }

    public void registerExternal(ExternalChannel channel) {
        topicMap.put(channel.pvName, channel.mqttTopic);
        mqttService.subscribe(channel.mqttTopic)
                .onItem().transformToUniAndConcatenate(message ->
                        parseMessage(message)
                                .chain(this::handleExternal)
                                .onFailure().recoverWithNull()
                )
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    public void removeExternal(String pvName) {
        mqttService.unsubscribe(topicMap.get(pvName));
    }

    public void registerAll(Collection<HostedChannel> hostedChannels) {
        if (hostedChannels != null)
            for (HostedChannel hostedChannel : hostedChannels) {
                for (Adapter a : adapterInstances) {
                    if (a instanceof ChannelAccessAdapter) {
                        adapters.put(hostedChannel.pvName, a);
                        if (hostedChannel.monitor)
                            a.monitorHosted(hostedChannel.pvName)
                                    .onItem().transformToUniAndConcatenate(this::update)
                                    .onFailure().recoverWithItem(unused -> null)
                                    .subscribe().with(unused -> {
                                    }, e -> Log.error("Monitor stream error", e));
                    }
                }
            }

        // noLocal subscription should prevent hosted channels from receiving their own updates
        mqttService.subscribe("pv/#")
                .onItem().transformToUniAndConcatenate(message ->
                        parseMessage(message)
                                .chain(this::handleExternal)
                                .onFailure().recoverWithNull()
                )
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    private Uni<Boolean> parseBooleanMessage(SignedMessage message) {
        try {
            return Uni.createFrom().item(mapper.readValue(message.getPayloadAsBytes(), Boolean.class));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    private Uni<PV> parseMessage(SignedMessage message) {
        try {
            PV pv = mapper.readValue(message.getPayloadAsBytes(), PV.class);
            pv.topic = message.getTopic();
            return Uni.createFrom().item(pv);
        } catch (Exception e) {
            Log.warnf(e, "Error deserializing MQTT payload from %s", message.getTopic());
            return Uni.createFrom().failure(e);
        }
    }

    private void addMonitor(HostedChannel channel, Adapter adapter) {
        monitors.put(channel.mqttTopic, adapter.monitorHosted(channel.pvName)
                .onItem().transformToUniAndConcatenate(this::update)
                .onFailure().recoverWithItem(unused -> null)
                .subscribe().with(unused -> {
                }, e -> Log.error("Monitor stream error", e)));
    }

    private Uni<Void> handledHostedPut(PV pv) {
        if (!adapters.containsKey(pv.pvName)) {
            Log.warnf("No adapter registered for hosted channel %s", pv.pvName);
            return Uni.createFrom().failure(new RuntimeException("No adapter registered for hosted channel " + pv.pvName));
        }

        Adapter adapter = adapters.get(pv.pvName);
        return adapter.putHosted(pv)
                .chain(unused -> adapter.getHosted(pv.pvName))
                .onItem().transform(pvValue -> new PV(pv.pvName, pvValue))
                .chain(this::update)
                .ifNoItem().after(Duration.ofSeconds(5)).failWith(TimeoutException::new);
    }

    private Uni<Void> handleExternal(PV pv) {
        topicMap.putIfAbsent(pv.pvName, pv.topic);
        pvCache.add(pv);
        return Uni.createFrom().nullItem();
    }

    /**
     * Only retrieves the in-memory value for the PV,
     * Requests it over MQTT if that doesn't exist
     */
    public Uni<PVValue> getExternalCached(String channel) {
        return pvCache.getCached(topicMap.get(channel));
    }

    /**
     * Requests the value of the PV over MQTT,
     * unless it is monitored (in which case the cache should be up to date)
     */
    public Uni<PVValue> getExternal(String channel) {
        return pvCache.get(topicMap.get(channel));
    }

    public Uni<Void> update(PV pv) {
        return mqttService.publish(topicMap.get(pv.pvName), pv);
    }

    public Uni<Void> putExternal(PV pv) {
        return mqttService.publish(topicMap.get(pv.pvName) + "/PUT", pv);
    }

    public Multi<PV> monitorExternal(String channel) {
        return mqttService.subscribe(topicMap.get(channel))
                .onSubscription().call(() -> mqttService.publish(topicMap.get(channel) + "/MONITOR", "true"))
                .onItem().transformToUniAndConcatenate(this::parseMessage);
    }

    public void clear() {
        pvCache.clear();
        adapters.clear();
    }
}
