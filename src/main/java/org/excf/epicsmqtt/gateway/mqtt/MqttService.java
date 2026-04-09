package org.excf.epicsmqtt.gateway.mqtt;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5RxClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import io.quarkus.logging.Log;
import io.quarkus.oidc.client.OidcClient;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.security.UnauthorizedException;
import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.reactivestreams.FlowAdapters;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

@ApplicationScoped
public class MqttService {
    @Inject
    OidcClient oidcClient;

    @ConfigProperty(name = "mqtt.host")
    String host;

    @ConfigProperty(name = "mqtt.port")
    int port;

    @ConfigProperty(name = "mqtt.client-id")
    String clientId;

    @ConfigProperty(name = "mqtt.subscriptions.nolocal", defaultValue = "true")
    Boolean noLocal;

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


    public Multi<MQTTMessage> subscribe(String topicFilter) {
        Mqtt5Subscribe subscribeMessage = Mqtt5Subscribe.builder()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_LEAST_ONCE)
                .noLocal(noLocal)
                .build();

        return Multi.createFrom().publisher(
                        FlowAdapters.toFlowPublisher(
                                client.subscribePublishes(subscribeMessage)
                        )
                )
                .onItem().transform(MQTTMessage::new)
                .skip().repetitions();
    }

    public Uni<Void> publish(String topic, byte[] payload, boolean retain) {
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
                                                    .payload(payload)
                                                    .userProperties()
                                                    .add("UUID", UUID.randomUUID().toString())
                                                    .applyUserProperties()
                                                    .build()
                                    ))
                            )
                    );
                })
                .onFailure().invoke(th -> Log.warn("MQTT publish failed, retrying..."))
                .onFailure().retry()
                .withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(10))
                .atMost(10)
                .onItem().transform(Unchecked.function(result -> {
                    if (result instanceof Mqtt5PublishResult.Mqtt5Qos1Result qos1Result){
                        if (qos1Result.getPubAck().getReasonCode() == Mqtt5PubAckReasonCode.NOT_AUTHORIZED)
                            throw new UnauthorizedException("MQTT publish failed, publishing to %s is not authorized".formatted(topic));
                        return qos1Result;
                    }
                    return result;
                }))
                .onItem().invoke(result -> Log.info(((Mqtt5PublishResult.Mqtt5Qos1Result) result).getPubAck().getReasonCode()))
                .replaceWithVoid();
    }

    private Uni<Void> tokenRefreshLoop() {
        return oidcClient.getTokens()
                .onFailure().retry()
                .withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(10))
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

    @ConfigProperty(name = "mqtt.session.expiry.interval", defaultValue = "3600")
    int sessionExpiryInterval;

    private Uni<Void> reconnectMqttClient(String accessToken) {
        return Uni.createFrom().deferred(() -> {

                    Mqtt5Connect connectMsg = Mqtt5Connect.builder()
                            .cleanStart(false)
                            .sessionExpiryInterval(sessionExpiryInterval)
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
