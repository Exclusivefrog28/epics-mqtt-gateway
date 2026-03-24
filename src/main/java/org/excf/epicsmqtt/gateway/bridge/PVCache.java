package org.excf.epicsmqtt.gateway.bridge;

import gov.aps.jca.Monitor;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.TimeoutException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
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

    private MultiEmitter<PV> globalEmitter;
    private final Multi<PV> hotStream;

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
                    failure -> Log.errorf(failure, "Failed to send GET request to %s", t)
            );

            return newFuture;
        });

        return Uni.createFrom().completionStage(future)
                .ifNoItem().after(Duration.ofSeconds(5)).failWith(TimeoutException::new)
                .onFailure(TimeoutException.class).invoke(unused -> pending.remove(topic, future));
    }

    public void add(PV pv) {
        PVValue oldValue = cache.get(pv.topic);
        if (oldValue != null){
            if (oldValue.value != pv.pvValue.value)
                pv.pvValue.changeMask |= Monitor.VALUE;

            if (oldValue.status != pv.pvValue.status || oldValue.severity != pv.pvValue.severity)
                pv.pvValue.changeMask |= Monitor.ALARM;

            if (oldValue.metadata != pv.pvValue.metadata)
                pv.pvValue.changeMask |= Monitor.PROPERTY;
        }else
            pv.pvValue.changeMask = Monitor.VALUE | Monitor.ALARM | Monitor.PROPERTY;

        cache.put(pv.topic, pv.pvValue);
        CompletableFuture<PVValue> future = pending.remove(pv.topic);

        if (future != null)
            future.complete(pv.pvValue);

        globalEmitter.emit(pv);
    }

    public Multi<PVValue> monitor(String topic) {
        return hotStream
                .filter(pv -> pv.topic.equals(topic))
                .map(pv -> pv.pvValue);
    }

    public PVCache() {
        hotStream = Multi.createFrom().<PV>emitter(
                emitter -> globalEmitter = (MultiEmitter<PV>) emitter,
                BackPressureStrategy.BUFFER
        ).broadcast().toAllSubscribers();
    }

    public void clear() {
        cache.clear();
        pending.clear();
    }

    private Uni<Void> sendGetRequest(String topic) {
        return mqtt.publishBoolean(topic + "/GET", true);
    }

}
