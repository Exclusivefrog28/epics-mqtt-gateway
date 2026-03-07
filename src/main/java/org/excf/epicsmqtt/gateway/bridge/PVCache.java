package org.excf.epicsmqtt.gateway.bridge;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.TimeoutException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.excf.epicsmqtt.gateway.mqtt.MQTTAdapter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Singleton
public class PVCache {
    private final ConcurrentMap<String, PVValue> cache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CompletableFuture<PVValue>> pending = new ConcurrentHashMap<>();
    private final BroadcastProcessor<PV> eventBus = BroadcastProcessor.create();

    @Inject
    MQTTAdapter mqtt;

    public Uni<PVValue> getCached(String topic) {
        if (topic == null) return Uni.createFrom().failure(new IllegalArgumentException("Topic cannot be null"));
        if (cache.containsKey(topic)) return Uni.createFrom().item(() -> cache.get(topic));
        else return get(topic);
    }

    public Uni<PVValue> get(String topic) {
        if (topic == null) return Uni.createFrom().failure(new IllegalArgumentException("Topic cannot be null"));
        CompletableFuture<PVValue> future = pending.computeIfAbsent(topic, t -> {
            CompletableFuture<PVValue> newFuture = new CompletableFuture<>();
            sendGetRequest(t).subscribe().with(
                    unused -> {
                    },
                    failure -> Log.errorf(failure, "Failied to send GET request to %s", t)
            );

            return newFuture;
        });

        return Uni.createFrom().completionStage(future)
                .ifNoItem().after(Duration.ofSeconds(10)).failWith(TimeoutException::new)
                .onFailure(TimeoutException.class).invoke(unused -> pending.remove(topic, future));
    }

    public void add(PV pv) {
        cache.put(pv.topic, pv.pvValue);
        CompletableFuture<PVValue> future = pending.remove(pv.topic);

        if (future != null)
            future.complete(pv.pvValue);

        eventBus.onNext(pv);
    }

    public Multi<PVValue> monitor(String topic) {
        return eventBus.toHotStream()
                .filter(pv -> pv.topic.equals(topic))
                .map(pv -> pv.pvValue);
    }

    public void clear() {
        cache.clear();
        pending.clear();
    }

    private Uni<Void> sendGetRequest(String topic) {
        return mqtt.publishBoolean(topic + "/GET", true);
    }

}
