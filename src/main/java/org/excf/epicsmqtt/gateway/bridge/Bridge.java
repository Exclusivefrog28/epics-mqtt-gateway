package org.excf.epicsmqtt.gateway.bridge;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.config.Channel;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class Bridge {

    protected Map<String, Adapter> adapters = new HashMap<>();
    protected Map<String, Channel> topicMap = new ConcurrentHashMap<>();

    protected Map<String, String> externalChannelValues = new ConcurrentHashMap<>();

    @Inject
    Instance<Adapter> adapterInstances;

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

    @Incoming("test/topic")
    public CompletionStage<Void> consume(MqttMessage<byte[]> message) {
        String topic = message.getTopic();

        Channel channel = topicMap.get(topic);
        if (channel == null) {
            return message.ack();
        }

        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);

        try {
            if (adapters.containsKey(channel.pvName)) {
                Adapter adapter = adapters.get(channel.pvName);

                if (adapter.hostsChannel(channel.pvName))
                    adapter.putHosted(channel.pvName, payload);

                else externalChannelValues.put(channel.pvName, payload);
            }
        } catch (Exception e) {
            Log.error(e);
        }

        return message.ack();
    }

    public String get(String channel) {
        return externalChannelValues.get(channel);
    }

    public void put(String channel, String value) {
        externalChannelValues.put(channel, value);
    }

}
