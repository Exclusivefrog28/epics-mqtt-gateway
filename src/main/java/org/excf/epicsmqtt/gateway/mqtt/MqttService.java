package org.excf.epicsmqtt.gateway.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5RxClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import io.quarkus.logging.Log;
import io.quarkus.oidc.client.OidcClient;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.reactivestreams.FlowAdapters;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

@ApplicationScoped
public class MqttService {
    @Inject
    OidcClient oidcClient;

    @Inject
    ObjectMapper mapper;

    @ConfigProperty(name = "mqtt.host")
    String host;

    @ConfigProperty(name = "mqtt.port")
    int port;

    @ConfigProperty(name = "mqtt.client-id")
    String clientId;

    private Mqtt5RxClient client;

    @PostConstruct
    void init() {
        Log.info("Initializing MQTT Client...");

        client = MqttClient.builder()
                .useMqttVersion5()
                .identifier(clientId)
                .serverHost(host)
                .serverPort(port)
                .sslWithDefaultConfig()
                .automaticReconnectWithDefaultConfig()
                .buildRx();
    }

    void onStart(@Observes StartupEvent ev) {
        Log.info("Connecting MQTT client for the first time");

        connectWithFreshToken().subscribe().with(
                success -> Log.info("MQTT Initial Connection Successful"),
                failure -> Log.error("MQTT Initial Connection Failed", failure)
        );
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (client != null) {
            client.disconnect().blockingAwait();
        }
    }


    public Multi<Mqtt5Publish> subscribe(String topicFilter) {
        Mqtt5Subscribe subscribeMessage = Mqtt5Subscribe.builder()
                .topicFilter(topicFilter)
                .build();

        return Multi.createFrom().publisher(
                FlowAdapters.toFlowPublisher(
                        client.subscribePublishes(subscribeMessage)
                )
        );
    }

    public Uni<Void> publish(String topic, PVValue pvValue) throws JsonProcessingException {
        byte[] payload = mapper.writeValueAsBytes(pvValue);

        return doPublish(topic, payload);
    }

    private Uni<Void> doPublish(String topic, byte[] payload) {
        return Uni.createFrom().deferred(() -> {
                    if (client == null || !client.getState().isConnected())
                        return Uni.createFrom().failure(new RuntimeException("Client Disconnected"));

                    return Uni.createFrom().publisher(
                            FlowAdapters.toFlowPublisher(
                                    client.publish(Flowable.just(
                                            Mqtt5Publish.builder()
                                                    .topic(topic)
                                                    .qos(MqttQos.AT_LEAST_ONCE)
                                                    .retain(true)
                                                    .payload(payload)
                                                    .build()
                                    ))
                            )
                    );
                })
                .onFailure().invoke(th -> Log.warnf("MQTT publish failed, retrying..."))
                .onFailure().retry()
                .withBackOff(Duration.ofSeconds(1))
                .atMost(10)
                .replaceWithVoid();
    }


    @Scheduled(every = "4m", delayed = "4m")
    void refreshAuth() {
        Log.info("Refreshing OIDC Token...");
        connectWithFreshToken().subscribe().with(
                success -> Log.info("Token refreshed and client re-connected."),
                failure -> Log.error("Token refresh failed", failure)
        );
    }

    private Uni<Boolean> connectWithFreshToken() {
        return oidcClient.getTokens()
                .chain(tokens -> {
                    String accessToken = tokens.getAccessToken();

                    Mqtt5Connect connectMsg = Mqtt5Connect.builder()
                            .cleanStart(false)
                            .sessionExpiryInterval(3600)
                            .simpleAuth()
                            .username(clientId)
                            .password(accessToken.getBytes(StandardCharsets.UTF_8))
                            .applySimpleAuth()
                            .build();

                    Uni<Void> disconnectStep = Uni.createFrom().voidItem();

                    if (client.getState().isConnected()) {
                        disconnectStep = Uni.createFrom().publisher(
                                FlowAdapters.toFlowPublisher(client.disconnect().toFlowable())
                        ).replaceWithVoid();
                    }

                    return disconnectStep
                            .chain(() -> Uni.createFrom().publisher(
                                    FlowAdapters.toFlowPublisher(client.connect(connectMsg).toFlowable())
                            ))
                            .map(connAck -> true);
                });
    }


}
