package org.excf.epicsmqtt.gateway.bridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
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

    protected Map<String, Adapter> adapters = new HashMap<>();
    protected Map<String, String> topicMap = new ConcurrentHashMap<>();
    protected Map<String, PVValue> externalChannelValues = new ConcurrentHashMap<>();

    @Inject
    Instance<Adapter> adapterInstances;

    @Inject
    MqttService mqttService;

    public void registerHosted(HostedChannel channel) {
        topicMap.put(channel.pvName, channel.mqttTopic);
        for (Adapter a : adapterInstances) {
            if (a instanceof ChannelAccessAdapter) {
                adapters.put(channel.pvName, a);
                if (channel.monitor) a.monitorHosted(channel.pvName);
            }
        }
        mqttService.subscribe(channel.mqttTopic)
                .onItem().transformToUniAndConcatenate(message ->
                        parseMessage(message)
                                .chain(this::handledHosted)
                                .onFailure().recoverWithNull()
                )
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    public void removeHosted(String pvName) {
        adapters.remove(pvName);
        mqttService.unsubscribe(pvName);
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
        mqttService.unsubscribe(pvName);
    }

    public void registerAll(Collection<HostedChannel> hostedChannels) {
        if (hostedChannels != null)
            for (HostedChannel hostedChannel : hostedChannels) {
                for (Adapter a : adapterInstances) {
                    if (a instanceof ChannelAccessAdapter) {
                        adapters.put(hostedChannel.pvName, a);
                        if (hostedChannel.monitor) a.monitorHosted(hostedChannel.pvName);
                    }
                }
            }

        mqttService.subscribe("pv/#")
                .onItem().transformToUniAndConcatenate(message ->
                        parseMessage(message)
                                .chain(this::handleMessage)
                                .onFailure().recoverWithNull()
                )
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    private Uni<PV> parseMessage(Mqtt5Publish message) {
        try {
            PV pv = mapper.readValue(message.getPayloadAsBytes(), PV.class);
            pv.topic = message.getTopic().toString();
            return Uni.createFrom().item(pv);
        } catch (Exception e) {
            Log.warnf("Error deserializing MQTT payload from %s", message.getTopic(), e);
            return Uni.createFrom().failure(e);
        }
    }

    private Uni<Void> handleMessage(PV pv) {
        if (adapters.containsKey(pv.pvName)) {
            return handledHosted(pv);
        } else {
            return handleExternal(pv);
        }
    }

    private Uni<Void> handledHosted(PV pv) {
        if (!adapters.containsKey(pv.pvName)) {
            Log.warnf("No adapter registered for hosted channel %s", pv.pvName);
            return Uni.createFrom().failure(new RuntimeException("No adapter registered for hosted channel " + pv.pvName));
        }

        Adapter adapter = adapters.get(pv.pvName);
        // TODO: handle non-monitoring hosted channels, without infinite calls,
        //       MQTT has noLocal subscribe, but I need local for testing
        return adapter.putHosted(pv)
                .ifNoItem().after(Duration.ofSeconds(3)).failWith(TimeoutException::new);
    }

    private Uni<Void> handleExternal(PV pv) {
        topicMap.putIfAbsent(pv.pvName, pv.topic);
        externalChannelValues.put(pv.pvName, pv.pvValue);
        return Uni.createFrom().nullItem();
    }

    /**
     * Only retrieves the in-memory value for the PV,
     * TODO: should do an MQTT call if that doesn't exist
     */
    public Uni<PVValue> getExternalCached(String channel) {
        return Uni.createFrom().item(() -> externalChannelValues.get(channel));
    }

    /**
     * TODO: do an MQTT call to get the value,
     *       if the value is monitored, the cache should have it already
     */
    public Uni<PVValue> getExternal(String channel) {
        return Uni.createFrom().item(() -> externalChannelValues.get(channel));
    }

    public Uni<Void> put(PV pv) {
        try {
            return mqttService.publish(topicMap.get(pv.pvName), pv);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot serialize PV", e);
        }
    }

    public Uni<Void> putExternal(PV pv) {
        externalChannelValues.put(pv.pvName, pv.pvValue);
        return put(pv);
    }

    public void clear() {
        externalChannelValues.clear();
        adapters.clear();
    }
}
