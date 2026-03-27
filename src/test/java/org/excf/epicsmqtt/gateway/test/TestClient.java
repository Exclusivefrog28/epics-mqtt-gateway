package org.excf.epicsmqtt.gateway.test;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.mqtt.MQTTAdapter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;


@ApplicationScoped
public class TestClient {

    @Inject
    MQTTAdapter mqttAdapter;

    ConcurrentMap<String, Cancellable> subscriptions = new ConcurrentHashMap<>();

    public void addPV(String topic, PV pv) {
        subscriptions.put(topic + "/GET",
                mqttAdapter.subscribeAndMerge(topic + "/GET",
                        (unused) -> mqttAdapter.publishPV(topic, pv, true)
                )
        );
    }

    public void removePV(String topic) {
        unsubscribe(topic + "/GET");
    }

    public AtomicReference<byte[]> subscribe(String topic) {

        AtomicReference<byte[]> lastMessage = new AtomicReference<>(null);

        subscriptions.put(topic,
                mqttAdapter.subscribeAndConcatenate(topic,
                        (message) -> Uni.createFrom().item(message)
                                .onItem().invoke(m -> lastMessage.set(m.getPayloadAsBytes())
                                )
                )
        );

        return lastMessage;
    }

    public void unsubscribe(String topic) {
        subscriptions.remove(topic).cancel();
    }

}
