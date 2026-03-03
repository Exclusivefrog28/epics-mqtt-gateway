package org.excf.epicsmqtt.gateway.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5RxClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import io.quarkus.logging.Log;
import io.quarkus.oidc.client.OidcClient;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.excf.epicsmqtt.gateway.model.PV;
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
                .buildRx();
    }

    void onStart(@Observes StartupEvent ev) {
        Log.info("Connecting MQTT client for the first time");

        tokenRefreshLoop()
                .subscribe().with(
                        unused -> {
                        },
                        failure -> Log.error("Unrecoverable error in auth loop.", failure)
                );
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (client != null && client.getState() != MqttClientState.DISCONNECTED) {
            client.disconnect().blockingAwait();
        }
    }


    public Multi<SignedMessage> subscribe(String topicFilter) {
        Mqtt5Subscribe subscribeMessage = Mqtt5Subscribe.builder()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_LEAST_ONCE)
                .build();

        return Multi.createFrom().publisher(
                        FlowAdapters.toFlowPublisher(
                                client.subscribePublishes(subscribeMessage)
                        )
                )
                .onItem().transform(SignedMessage::new)
                .skip().repetitions();
    }

    public void unsubscribe(String topicFilter) {
        Uni.createFrom().publisher(
                FlowAdapters.toFlowPublisher(
                        client.unsubscribeWith().topicFilter(topicFilter).applyUnsubscribe().toFlowable()
                )
        ).await().indefinitely();
    }

    public Uni<Void> publish(String topic, PV pv) {
        try {
            byte[] payload = mapper.writeValueAsBytes(pv);
            return doPublish(topic, payload, true);
        } catch (JsonProcessingException e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> publish(String topic, String payload) {
        return doPublish(topic, payload.getBytes(StandardCharsets.UTF_8), false);
    }

    private Uni<Void> doPublish(String topic, byte[] payload, boolean retain) {
        return Uni.createFrom().deferred(() -> {
                    if (client == null || !client.getState().isConnected())
                        return Uni.createFrom().failure(new RuntimeException("Client Disconnected"));

                    return Uni.createFrom().publisher(
                            FlowAdapters.toFlowPublisher(
                                    client.publish(Flowable.just(
                                            Mqtt5Publish.builder()
                                                    .topic(topic)
                                                    .qos(MqttQos.AT_LEAST_ONCE)
                                                    .retain(retain)
                                                    .payload(SignedMessage.signMessage(payload))
                                                    .build()
                                    ))
                            )
                    );
                })
                .onFailure().invoke(th -> Log.warn("MQTT publish failed, retrying..."))
                .onFailure().retry()
                .withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(2))
                .atMost(10)
                .replaceWithVoid();
    }

    private Uni<Void> tokenRefreshLoop() {
        return oidcClient.getTokens()
                .onFailure().retry()
                .withBackOff(Duration.ofSeconds(2), Duration.ofSeconds(15))
                .atMost(5)
                .chain(tokens -> {
                    Log.info("Token acquired");
                    String accessToken = tokens.getAccessToken();

                    return reconnectMqttClient(accessToken)
                            .chain(() -> {
                                // next refresh 30 seconds before the token expires
                                long delayToNextRefresh = Math.max(1, tokens.getAccessTokenExpiresAt() - System.currentTimeMillis() / 1000 - 30);
                                Log.info("MQTT Connected. Next refresh scheduled in %d seconds.".formatted(delayToNextRefresh));

                                return Uni.createFrom().voidItem()
                                        .onItem().delayIt().by(Duration.ofSeconds(delayToNextRefresh))
                                        .onItem().invoke(unused -> Log.info("Refreshing MQTT connection..."))
                                        .chain(this::tokenRefreshLoop);
                            });
                });
    }

    private Uni<Void> reconnectMqttClient(String accessToken) {
        return Uni.createFrom().deferred(() -> {

                    Mqtt5Connect connectMsg = Mqtt5Connect.builder()
                            .cleanStart(false)
                            .sessionExpiryInterval(3600)
                            .simpleAuth()
                            .username(clientId)
                            .password(accessToken.getBytes(StandardCharsets.UTF_8))
                            .applySimpleAuth()
                            .build();

                    Uni<Void> disconnectStep = Uni.createFrom().voidItem();

                    if (client.getState() != MqttClientState.DISCONNECTED) {
                        disconnectStep = Uni.createFrom().publisher(
                                FlowAdapters.toFlowPublisher(client.disconnect().toFlowable())
                        ).onFailure().recoverWithNull().replaceWithVoid();
                    }

                    return disconnectStep
                            .chain(() -> Uni.createFrom().publisher(
                                    FlowAdapters.toFlowPublisher(client.connect(connectMsg).toFlowable())
                            ))
                            .replaceWithVoid();
                })
                .onFailure().retry()
                .withBackOff(Duration.ofSeconds(2), Duration.ofSeconds(10))
                .atMost(5);
    }


}
