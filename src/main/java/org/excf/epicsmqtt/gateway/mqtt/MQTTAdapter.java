package org.excf.epicsmqtt.gateway.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.model.PV;

import java.util.function.Function;

@ApplicationScoped
public class MQTTAdapter {
    @Inject
    MqttService mqttService;

    @Inject
    ObjectMapper mapper;

    public Uni<Boolean> parseBooleanMessage(MQTTMessage message) {
        try {
            return Uni.createFrom().item(mapper.readValue(message.getPayloadAsBytes(), Boolean.class));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<PV> parseMessage(MQTTMessage message) {
        try {
            PV pv = mapper.readValue(message.getPayloadAsBytes(), PV.class);
            pv.topic = message.getTopic();
            return Uni.createFrom().item(pv);
        } catch (Exception e) {
            Log.warnf(e, "Error deserializing MQTT payload from %s", message.getTopic());
            return Uni.createFrom().failure(e);
        }
    }

    public <O> Cancellable subscribeAndMerge(String topic, Function<MQTTMessage, Uni<? extends O>> uniMapper) {
        return mqttService.subscribe(topic)
                .onItem().transformToUniAndConcatenate(message ->
                        uniMapper.apply(message).onFailure().recoverWithNull()
                )
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    public <O> Cancellable subscribeAndConcatenate(String topic, Function<MQTTMessage, Uni<? extends O>> uniMapper) {
        return mqttService.subscribe(topic)
                .onItem().transformToUniAndConcatenate(message ->
                        uniMapper.apply(message).onFailure().recoverWithNull()
                )
                .subscribe().with(unused -> {
                }, e -> Log.error("MQTT stream error", e));
    }

    public Uni<Void> publishPV(String topic, PV pv, boolean retain){
        try {
            byte[] payload = mapper.writeValueAsBytes(pv);
            return mqttService.publish(topic, payload, retain);
        } catch (JsonProcessingException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> publishBoolean(String topic, boolean value){
        try {
            byte[] payload = mapper.writeValueAsBytes(value);
            return mqttService.publish(topic, payload, false);
        } catch (JsonProcessingException e) {
            return Uni.createFrom().failure(e);
        }
    }

}
