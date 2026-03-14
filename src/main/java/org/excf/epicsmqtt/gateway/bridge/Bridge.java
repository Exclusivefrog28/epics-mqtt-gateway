package org.excf.epicsmqtt.gateway.bridge;

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
import org.excf.epicsmqtt.gateway.mqtt.MQTTAdapter;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

@ApplicationScoped
public class Bridge {

    @Inject
    MQTTAdapter mqttAdapter;

    @Inject
    PVCache pvCache;

    // map topics to adapters
    protected Map<String, Adapter> adapters = new HashMap<>();


    // map protocols and local names to topics
    protected ConcurrentMap<String, Map<String, String>> topicMap = new ConcurrentHashMap<>();

    // map topics to MQTT subscriptions
    protected Map<String, Collection<Cancellable>> subscriptions = new HashMap<>();
    // map topics to monitor streams
    protected Map<String, Cancellable> monitors = new HashMap<>();

    @Inject
    Instance<Adapter> adapterInstances;

    public void registerHosted(HostedChannel channel) {
        topicMap.computeIfAbsent(channel.protocol, (unused) -> new HashMap<>())
                .put(channel.getSourceName(), channel.mqttTopic);

        Collection<Cancellable> channelSubs = subscriptions.computeIfAbsent(channel.mqttTopic, (unused) -> new ArrayList<>());

        Adapter adapter = adapterInstances.select(NamedLiteral.of(channel.protocol)).get();

        adapters.put(channel.mqttTopic, adapter);
        if (channel.monitor)
            addMonitor(channel, adapter);
        else
            channelSubs.add(
                    mqttAdapter.subscribe(channel.mqttTopic + "/MONITOR", request ->
                            mqttAdapter.parseBooleanMessage(request)
                                    .invoke(shouldMonitor -> {
                                        if (shouldMonitor) addMonitor(channel, adapter);
                                    })
                    )
            );

        channelSubs.add(
                mqttAdapter.subscribe(channel.mqttTopic + "/GET", request ->
                        adapter.getHosted(channel.getSourceName())
                                .onItem().transform(pvValue -> new PV(channel.localNames, pvValue))
                                .chain((pv) -> this.update(channel.mqttTopic, pv))
                )
        );

        channelSubs.add(
                mqttAdapter.subscribe(channel.mqttTopic + "/PUT", request ->
                        mqttAdapter.parseMessage(request)
                                .chain((pv) -> adapter.putHosted(channel.getSourceName(), pv.pvValue))
                                .chain(unused -> adapter.getHosted(channel.getSourceName()))
                                .chain((pvValue) -> update(channel.mqttTopic, new PV(channel.localNames, pvValue)))
                                .ifNoItem().after(Duration.ofSeconds(5)).failWith(TimeoutException::new)
                )
        );
    }

    public void removeHosted(String topic) {
        if (monitors.containsKey(topic))
            monitors.remove(topic).cancel();

        adapters.remove(topic);

        Collection<Cancellable> channelSubs = subscriptions.remove(topic);
        if (channelSubs != null)
            channelSubs.forEach(Cancellable::cancel);
    }

    public void registerExternal(ExternalChannel channel) {
        // if protocol names are configured, they will be added to the map,
        // otherwise they're only loaded once a message is received
        if (channel.localNames != null)
            channel.localNames.forEach((protocol, localName) ->
                    topicMap.computeIfAbsent(protocol, (unused) -> new HashMap<>())
                            .put(localName, channel.mqttTopic)
            );

        subscriptions.put(channel.mqttTopic, List.of(
                mqttAdapter.subscribe(channel.mqttTopic, message ->
                        mqttAdapter.parseMessage(message).chain(this::handleExternal)
                )
        ));
    }

    public void removeExternal(String topic) {
        topicMap.remove(topic);
        Collection<Cancellable> channelSubs = subscriptions.remove(topic);
        if (channelSubs != null)
            channelSubs.forEach(Cancellable::cancel);
    }

    public void registerAll(Collection<HostedChannel> hostedChannels) {
        if (hostedChannels != null)
            for (HostedChannel hostedChannel : hostedChannels)
                registerHosted(hostedChannel);

        // noLocal subscription should prevent hosted channels from receiving their own updates
        mqttAdapter.subscribe("pv/+", message ->
                mqttAdapter.parseMessage(message).chain(this::handleExternal)
        );
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
        if (topicMap.get(protocol) == null) return Uni.createFrom().failure(new IllegalArgumentException("Protocol not registered"));
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
        return mqttAdapter.publishPV(topic, pv, true);
    }

    public Uni<Void> putExternal(String protocol, String localName, PVValue pvValue) {
        return mqttAdapter.publishPV(topicMap.get(protocol).get(localName) + "/PUT", new PV(pvValue), false);
    }

    public Multi<PVValue> monitorExternal(String protocol, String localName) {
        String baseTopic = topicMap.get(protocol).get(localName);

        return pvCache.monitor(baseTopic)
                .onSubscription().call(() -> mqttAdapter.publishBoolean(baseTopic + "/MONITOR", true));
    }

    public void clear() {
        pvCache.clear();
        adapters.clear();
    }
}
