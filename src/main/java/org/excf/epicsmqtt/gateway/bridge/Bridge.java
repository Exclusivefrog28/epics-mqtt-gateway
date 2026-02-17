package org.excf.epicsmqtt.gateway.bridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        mqttService.subscribe(channel.mqttTopic).subscribe().with(this::handleMessage, e -> Log.error("Stream error", e));
    }

    public void registerExternal(ExternalChannel channel) {
        topicMap.put(channel.mqttTopic, channel);
        for (Adapter a : adapterInstances) {
            if (a instanceof ChannelAccessAdapter) {
                a.addExternalChannel(channel.pvName);
                adapters.put(channel.pvName, a);
            }
        }
        mqttService.subscribe(channel.mqttTopic).subscribe().with(this::handleMessage, e -> Log.error("Stream error", e));
    }

    public void handleMessage(Mqtt5Publish message) {
        String topic = message.getTopic().toString();

        ExternalChannel channel = topicMap.get(topic);
        if (channel == null) {
            return;
        }

        try {
            PVValue pvValue = mapper.readValue(message.getPayloadAsBytes(), PVValue.class);

            if (adapters.containsKey(channel.pvName)) {
                Adapter adapter = adapters.get(channel.pvName);

                if (adapter.hostsChannel(channel.pvName) && channel instanceof HostedChannel hostedChannel) {
                    if (!hostedChannel.monitor) adapter.putHosted(channel.pvName, pvValue);
                } else
                    externalChannelValues.put(channel.pvName, pvValue);
            }
        } catch (Exception e) {
            Log.error("Error processing MQTT message", e);
        }
        ;
    }

    public PVValue getExternal(String channel) {
        return externalChannelValues.get(channel);
    }

    public void put(String pvName, PVValue value) {
        try {
            mqttService.publish(getChannel(pvName).mqttTopic, value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot serialize PVValue", e);
        }
    }

    public void putExternal(String pvName, PVValue value) {
        externalChannelValues.put(pvName, value);
        put(pvName, value);
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
