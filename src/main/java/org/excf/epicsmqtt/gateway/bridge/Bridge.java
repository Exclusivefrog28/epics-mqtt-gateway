package org.excf.epicsmqtt.gateway.bridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.config.ExternalChannel;
import org.excf.epicsmqtt.gateway.config.HostedChannel;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.excf.epicsmqtt.gateway.mqtt.MqttService;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@ApplicationScoped
public class Bridge {

    @Inject
    ObjectMapper mapper;

    protected Map<String, Adapter> adapters = new HashMap<>();
    protected Map<String, ExternalChannel> topicMap = new ConcurrentHashMap<>();

    protected Map<String, PVValue> externalChannelValues = new ConcurrentHashMap<>();

    @Inject
    Instance<Adapter> adapterInstances;

    @Inject
    MqttService mqttService;

    void onStop(@Observes ShutdownEvent ev) {
    }

    public void registerHosted(HostedChannel channel) {
        topicMap.put(channel.mqttTopic, channel);
        for (Adapter a : adapterInstances) {
            if (a instanceof ChannelAccessAdapter) {
                a.addHostedChannel(channel.pvName, channel.monitor);
                adapters.put(channel.pvName, a);
            }
        }
        mqttService.subscribe(channel.mqttTopic)
                .onItem().transformToUniAndConcatenate(this::handledHosted)
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    public void removeHosted(String pvName) {
        topicMap.remove(pvName);
        adapters.get(pvName).removeHostedChannel(pvName);
        adapters.remove(pvName);
        mqttService.unsubscribe(pvName);
    }

    public void registerExternal(ExternalChannel channel) {
        topicMap.put(channel.mqttTopic, channel);
        for (Adapter a : adapterInstances) {
            if (a instanceof ChannelAccessAdapter) {
                a.addExternalChannel(channel.pvName);
                adapters.put(channel.pvName, a);
            }
        }
        mqttService.subscribe(channel.mqttTopic)
                .onItem().transformToUniAndConcatenate(this::handleExternal)
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    public void removeExternal(String pvName) {
        topicMap.remove(pvName);
        adapters.get(pvName).removeExternalChannel(pvName);
        adapters.remove(pvName);
        mqttService.unsubscribe(pvName);
    }

    private Uni<Void> handledHosted(Mqtt5Publish message) {
        String topic = message.getTopic().toString();

        HostedChannel channel = (HostedChannel) topicMap.get(topic);
        if (channel == null) {
            Log.warnf("Hosted channel not registered for topic %s", topic);
            return Uni.createFrom().nullItem();
        }

        PVValue pvValue;
        try {
            pvValue = mapper.readValue(message.getPayloadAsBytes(), PVValue.class);
        } catch (Exception e) {
            Log.warnf("Error deserializing MQTT payload from %s", topic, e);
            return Uni.createFrom().nullItem();
        }

        if (!adapters.containsKey(channel.pvName)) {
            Log.warnf("No adapter registered for channel %s", channel.pvName);
            return Uni.createFrom().nullItem();
        }

        Adapter adapter = adapters.get(channel.pvName);
        // TODO: handle non-monitoring hosted channels, without infinite calls,
        //       MQTT has noLocal subscribe, but I need local for testing
        if (!channel.monitor) return adapter.putHosted(channel.pvName, pvValue)
                .ifNoItem().after(Duration.ofSeconds(1)).failWith(TimeoutException::new).onFailure().recoverWithNull();

        return Uni.createFrom().nullItem();
    }

    private Uni<Void> handleExternal(Mqtt5Publish message) {
        String topic = message.getTopic().toString();

        ExternalChannel channel = topicMap.get(topic);
        if (channel == null) {
            Log.warnf("External channel not registered for topic %s", topic);
            return Uni.createFrom().nullItem();
        }

        PVValue pvValue;
        try {
            pvValue = mapper.readValue(message.getPayloadAsBytes(), PVValue.class);
        } catch (Exception e) {
            Log.warnf("Error deserializing MQTT payload from %s", topic, e);
            return Uni.createFrom().nullItem();
        }

        externalChannelValues.put(channel.pvName, pvValue);
        return Uni.createFrom().nullItem();
    }

    /**
     * Only retrieves the in-memory value for the PV,
     * TODO: should do an MQTT call if that doesn't exist
     */
    public PVValue getExternalCached(String channel) {
        return externalChannelValues.get(channel);
    }

    /**
     * TODO: do an MQTT call to get the value,
     *       if the value is monitored, the cache should have it already
     */
    public Uni<PVValue> getExternal(String channel) {
        return Uni.createFrom().item(() -> externalChannelValues.get(channel));
    }

    public Uni<Void> put(String pvName, PVValue value) {
        try {
            return mqttService.publish(getChannel(pvName).mqttTopic, value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot serialize PVValue", e);
        }
    }

    public Uni<Void> putExternalAsync(String pvName, PVValue value) {
        externalChannelValues.put(pvName, value);
        return put(pvName, value);
    }

    public ExternalChannel getChannel(String pvName) {
        for (ExternalChannel c : topicMap.values()) {
            if (c.pvName.equals(pvName))
                return c;
        }
        return null;
    }

    public void clear() {
        externalChannelValues.clear();
        adapters.clear();
        topicMap.clear();
    }
}
