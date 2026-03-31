package org.excf.epicsmqtt.gateway.test.mqtt;

import io.quarkus.security.UnauthorizedException;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.mqtt.MQTTAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

@QuarkusTest
@Tag("mqtt")
public class MQTTTest {
    @Inject
    MQTTAdapter mqttAdapter;

    @Test
    public void unauthorizedTopicTest() {
        Assertions.assertThrows(UnauthorizedException.class, () -> mqttAdapter.publishBoolean("pv/unathorized", true).await().atMost(Duration.ofSeconds(10)));
    }
}
