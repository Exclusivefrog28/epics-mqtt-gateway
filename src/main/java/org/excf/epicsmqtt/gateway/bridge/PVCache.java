package org.excf.epicsmqtt.gateway.bridge;

import io.smallrye.mutiny.TimeoutException;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.excf.epicsmqtt.gateway.mqtt.MqttService;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class PVCache {
    private final Map<String, CompletableFuture<PVValue>> cache = new ConcurrentHashMap<>();

    @Inject
    MqttService mqttService;

    public Uni<PVValue> getCached(String topic) {
        if (topic == null) return Uni.createFrom().failure(new IllegalArgumentException("Topic cannot be null"));

        CompletableFuture<PVValue> future = cache.computeIfAbsent(topic, t -> {
            CompletableFuture<PVValue> pending = new CompletableFuture<>();

            sendGetRequest(t).await().atMost(Duration.ofSeconds(10));

            return pending;
        });

        return Uni.createFrom().completionStage(future)
                .ifNoItem().after(Duration.ofSeconds(10)).failWith(TimeoutException::new)
                .onFailure(TimeoutException.class).invoke(unused -> cache.remove(topic));
    }

    public Uni<PVValue> get(String topic) {
        CompletableFuture<PVValue> future = cache.compute(topic, (t, currentFuture) -> {
            if (currentFuture != null && !currentFuture.isDone())
                return currentFuture; // already waiting for a value, use the same future

            CompletableFuture<PVValue> pending = new CompletableFuture<>();

            sendGetRequest(t).await().atMost(Duration.ofSeconds(10));

            return pending;
        });

        return Uni.createFrom().completionStage(future)
                .ifNoItem().after(Duration.ofSeconds(10)).failWith(TimeoutException::new)
                .onFailure(TimeoutException.class).invoke(unused -> cache.remove(topic));
    }

    public void add(PV pv) {
        cache.compute(pv.topic, (k, currentFuture) -> {
            if (currentFuture != null && !currentFuture.isDone()) {
                currentFuture.complete(pv.pvValue);
                return currentFuture;
            } else
                return CompletableFuture.completedFuture(pv.pvValue);
        });
    }

    public void clear() {
        cache.clear();
    }

    private Uni<Void> sendGetRequest(String topic) {
        return mqttService.publish(topic + "/GET", "GET");
    }

}
