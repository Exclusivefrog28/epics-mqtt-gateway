package org.excf.epicsmqtt.gateway.test.opc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.excf.epicsmqtt.gateway.adapter.opc.OPCAdapter;
import org.excf.epicsmqtt.gateway.bridge.Bridge;
import org.excf.epicsmqtt.gateway.config.HostedChannel;
import org.excf.epicsmqtt.gateway.config.Mode;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.mqtt.MQTTAdapter;
import org.excf.epicsmqtt.gateway.test.TestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@QuarkusTest
@QuarkusTestResource(value = OpcServerResource.class, restrictToAnnotatedClass = true)
@Tag("opc")
@Tag("opc-core")
public class OPCCoreTest {

    @Inject
    Bridge bridge;

    @Inject
    OPCAdapter adapter;

    @Inject
    MQTTAdapter mqttAdapter;

    @Inject
    TestClient testClient;

    @Inject
    ObjectMapper mapper;

    @Test
    public void testHostedChannel() throws Exception {

        HostedChannel channel = new HostedChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/opc-hosted";
        channel.localNames = Map.of("opc", "ns=4;i=6217");
        channel.protocol = "opc";
        channel.mode = Mode.READ_WRITE;

        bridge.registerHosted(channel);

        //adapter.browseServer(NodeIds.ObjectsFolder, "");

        AtomicReference<byte[]> lastMessageRef = testClient.subscribe(channel.mqttTopic);
        mqttAdapter.publishBoolean(channel.mqttTopic + "/GET", true).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(() -> {
            byte[] lastMessage = lastMessageRef.get();
            Assertions.assertNotNull(lastMessage);
            float value = ((float[]) mapper.readValue(lastMessage, PV.class).pvValue.value)[0];
            Log.info(value);
        });

        PV pv = mapper.readValue(lastMessageRef.get(), PV.class);
        float randomInt = new Random().nextInt(100);
        pv.pvValue.value = new float[]{randomInt};

        mqttAdapter.publishPV(channel.mqttTopic + "/PUT", pv, false).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(() -> {
            byte[] lastMessage = lastMessageRef.get();
            Assertions.assertNotNull(lastMessage);
            float value = ((float[]) mapper.readValue(lastMessage, PV.class).pvValue.value)[0];
            Assertions.assertEquals(randomInt, value);
        });

        bridge.removeHosted(channel.mqttTopic);
    }
}
