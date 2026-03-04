package org.excf.epicsmqtt.gateway.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.config.ExternalChannel;
import org.excf.epicsmqtt.gateway.config.HostedChannel;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.excf.epicsmqtt.gateway.mqtt.MqttService;
import org.excf.epicsmqtt.gateway.mqtt.MQTTMessage;

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

    // map topics to adapters
    protected Map<String, Adapter> adapters = new HashMap<>();

    // map protocols and local names to topics
    protected Map<String, Map<String, String>> topicMap = new ConcurrentHashMap<>();

    // map topics to monitor streams
    protected Map<String, Cancellable> monitors = new HashMap<>();

    @Inject
    Instance<Adapter> adapterInstances;

    @Inject
    MqttService mqttService;

    public void registerHosted(HostedChannel channel) {
        topicMap.computeIfAbsent(channel.protocol, (unused) -> new HashMap<>())
                .put(channel.getSourceName(), channel.mqttTopic);

        Adapter adapter = adapterInstances.select(NamedLiteral.of(channel.protocol)).get();

        adapters.put(channel.mqttTopic, adapter);
        if (channel.monitor)
            addMonitor(channel, adapter);
        else
            mqttService.subscribe(channel.mqttTopic + "/MONITOR")
                    .onItem().transformToUniAndConcatenate(request ->
                            parseBooleanMessage(request)
                                    .onItem().invoke(shouldMonitor -> {
                                        if (shouldMonitor) addMonitor(channel, adapter);
                                    })
                    ).subscribe().with(unused -> {
                    }, e -> Log.error("MQTT stream error", e));

        mqttService.subscribe(channel.mqttTopic + "/GET")
                .onItem().transformToUniAndConcatenate(request ->
                        adapter.getHosted(channel.getSourceName())
                                .onItem().transform(pvValue -> new PV(channel.localNames, pvValue))
                                .chain((pv) -> this.update(channel.mqttTopic, pv))
                                .onFailure().recoverWithNull()
                ).subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));


        mqttService.subscribe(channel.mqttTopic + "/PUT")
                .onItem().transformToUniAndConcatenate(message ->
                        parseMessage(message)
                                .chain((pv) -> adapter.putHosted(channel.getSourceName(), pv.pvValue))
//                                .chain(unused -> adapter.getHosted(channel.getSourceName()))
//                                .chain((pvValue) -> update(channel.mqttTopic, new PV(channel.localNames, pvValue)))
                                .ifNoItem().after(Duration.ofSeconds(5)).failWith(TimeoutException::new)
                                .onFailure().recoverWithNull()
                ).subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    public void removeHosted(HostedChannel channel) {
        if (monitors.containsKey(channel.mqttTopic)) {
            monitors.get(channel.mqttTopic).cancel();
            monitors.remove(channel.mqttTopic);
        }
        adapters.remove(channel.mqttTopic);
        mqttService.unsubscribe(channel.mqttTopic);
    }

    public void registerExternal(ExternalChannel channel) {
        // if protocol names are configured, they will be added to the map,
        // otherwise they're only loaded once a message is received
        channel.localNames.forEach((protocol, localName) ->
                topicMap.computeIfAbsent(protocol, (unused) -> new HashMap<>())
                        .put(localName, channel.mqttTopic)
        );
        mqttService.subscribe(channel.mqttTopic)
                .onItem().transformToUniAndConcatenate(message ->
                        parseMessage(message)
                                .chain(this::handleExternal)
                                .onFailure().recoverWithNull()
                )
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    public void removeExternal(String topic) {
        // TODO: remove topicMap entries associated with the channel + remove monitors
        mqttService.unsubscribe(topic);
    }

    public void registerAll(Collection<HostedChannel> hostedChannels) {
        if (hostedChannels != null)
            for (HostedChannel hostedChannel : hostedChannels)
                registerHosted(hostedChannel);

        // noLocal subscription should prevent hosted channels from receiving their own updates
        mqttService.subscribe("pv/+")
                .onItem().transformToUniAndConcatenate(message ->
                        parseMessage(message)
                                .chain(this::handleExternal)
                                .onFailure().recoverWithNull()
                )
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    private Uni<Boolean> parseBooleanMessage(MQTTMessage message) {
        try {
            return Uni.createFrom().item(mapper.readValue(message.getPayloadAsBytes(), Boolean.class));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    private Uni<PV> parseMessage(MQTTMessage message) {
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
        monitors.put(channel.mqttTopic, adapter.monitorHosted(channel.getSourceName())
                .onItem().transformToUniAndConcatenate((pvValue) -> update(channel.mqttTopic, new PV(channel.localNames, pvValue, true)))
                .onFailure().recoverWithItem(unused -> null)
                .subscribe().with(unused -> {
                }, e -> Log.error("Monitor stream error", e)));
    }

    private Uni<Void> handleExternal(PV pv) {
        // adds unknown names to the topicMap but keeps original entries for local aliasing
        pv.localNames.forEach(topicMap.computeIfAbsent(pv.topic, unused -> new HashMap<>())::putIfAbsent);
        pvCache.add(pv);
        return Uni.createFrom().nullItem();
    }

    /**
     * Only retrieves the in-memory value for the PV,
     * Requests it over MQTT if that doesn't exist
     */
    public Uni<PVValue> getExternalCached(String protocol, String localName) {
        return pvCache.getCached(topicMap.get(protocol).get(localName));
    }

    /**
     * Requests the value of the PV over MQTT,
     * unless it is monitored (in which case the cache should be up to date)
     */
    public Uni<PVValue> getExternal(String protocol, String localName) {
        return pvCache.get(topicMap.get(protocol).get(localName));
    }

    public Uni<Void> update(String topic, PV pv) {
        return mqttService.publish(topic, pv);
    }

    public Uni<Void> putExternal(String protocol, String localName, PVValue pvValue) {
        return mqttService.publish(topicMap.get(protocol).get(localName) + "/PUT", new PV(pvValue), false);
    }

    public Multi<PV> monitorExternal(String protocol, String localName) {
        String baseTopic = topicMap.get(protocol).get(localName);
        return mqttService.subscribe(baseTopic)
                .onSubscription().call(() -> mqttService.publish(baseTopic + "/MONITOR", "true"))
                .onItem().transformToUniAndConcatenate(this::parseMessage);
    }

    public void clear() {
        pvCache.clear();
        adapters.clear();
    }
}
