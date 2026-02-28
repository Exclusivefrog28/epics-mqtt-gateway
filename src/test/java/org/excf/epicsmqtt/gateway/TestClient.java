package org.excf.epicsmqtt.gateway;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.mqtt.MqttService;

import java.util.concurrent.atomic.AtomicReference;


@ApplicationScoped
public class TestClient {

    @Inject
    MqttService mqttService;

    void addPV(String topic, PV pv){
        mqttService.subscribe(topic + "/GET")
                .onItem().transformToUniAndConcatenate(
                        unused -> mqttService.publish(topic, pv)
                ).subscribe().with(unused -> {});
    }

    void removePV(String topic){
        mqttService.unsubscribe(topic + "/GET");
    }

    AtomicReference<byte[]> subscribe(String topic){

        AtomicReference<byte[]> lastMessage = new AtomicReference<>(null);

        mqttService.subscribe(topic)
                .onItem().invoke(
                        message -> lastMessage.set(message.getPayloadAsBytes())
                ).subscribe().with(unused -> {
                });

        return lastMessage;
    }

    void unsubscribe(String topic){
        mqttService.unsubscribe(topic);
    }

}
