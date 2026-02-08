package org.excf.epicsmqtt.gateway.bridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.config.Channel;
import org.excf.epicsmqtt.gateway.model.PVValue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class Bridge {

    @Inject
    ObjectMapper mapper;

    protected Map<String, Adapter> adapters = new HashMap<>();
    protected Map<String, Channel> topicMap = new ConcurrentHashMap<>();

    protected Map<String, PVValue> externalChannelValues = new ConcurrentHashMap<>();

    @Inject
    Instance<Adapter> adapterInstances;

    @Inject
    @org.eclipse.microprofile.reactive.messaging.Channel("data-out")
    Emitter<byte[]> emitter;

    void onStop(@Observes ShutdownEvent ev) {
    }

    public void registerHosted(Channel channel) {
        topicMap.put(channel.mqttTopic, channel);
        for (Adapter a : adapterInstances) {
            if (a instanceof ChannelAccessAdapter) {
                a.addHostedChannel(channel.pvName);
                adapters.put(channel.pvName, a);
            }
        }
    }

    public void registerExternal(Channel channel) {
        topicMap.put(channel.mqttTopic, channel);
        for (Adapter a : adapterInstances) {
            if (a instanceof ChannelAccessAdapter) {
                a.addExternalChannel(channel.pvName);
                adapters.put(channel.pvName, a);
            }
        }
    }

    @Incoming("data-in")
    public CompletionStage<Void> consume(MqttMessage<byte[]> message) {
        String topic = message.getTopic();
        Log.info("Topic: %s".formatted(topic));

        Channel channel = topicMap.get(topic);
        if (channel == null) {
            return message.ack();
        }

        try {
            PVValue pvValue = mapper.readValue(message.getPayload(), PVValue.class);

            if (adapters.containsKey(channel.pvName)) {
                Adapter adapter = adapters.get(channel.pvName);

                if (adapter.hostsChannel(channel.pvName))
                    adapter.putHosted(channel.pvName, pvValue);

                else
                    externalChannelValues.put(channel.pvName, pvValue);
            }
        } catch (Exception e) {
            Log.error("Error processing MQTT message", e);
        }

        return message.ack();
    }

    public PVValue getExternal(String channel) {
        return externalChannelValues.get(channel);
    }

    public void put(String pvName, PVValue value){
        try {
            emitter.send(MqttMessage.of(getChannel(pvName).mqttTopic, mapper.writeValueAsBytes(value)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot serialize PVValue", e);
        }
    }

    public void putExternal(String pvName, PVValue value) {
        externalChannelValues.put(pvName, value);
        put(pvName, value);
    }

    public Channel getChannel(String pvName) {
        for (Channel c : topicMap.values()) {
            if (c.pvName.equals(pvName))
                return c;
        }
        return null;
    }
}
