package org.excf.epicsmqtt.gateway.bridge;

import com.cosylab.epics.caj.CAJContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.aps.jca.configuration.DefaultConfiguration;
import gov.aps.jca.dbr.*;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.excf.epicsmqtt.gateway.adapter.ca.CAClient;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.config.Channel;
import org.excf.epicsmqtt.gateway.config.Mode;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@QuarkusTest
public class ChannelAccessEnumTest {
    @Inject
    Bridge bridge;

    @Inject
    ChannelAccessAdapter adapter;

    @Inject
    @org.eclipse.microprofile.reactive.messaging.Channel("data-out")
    Emitter<byte[]> emitter;

    @Inject
    ObjectMapper mapper;

    @Inject
    ChannelAccessCoreTest.BrokerSpy spy;

    /**
     * Tests CA client get and put to local ENUM type channel
     */
    @Test
    public void testHostedChannelEnum() throws Exception {

        Channel channel = new Channel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/acquire";
        channel.pvName = "BL01T-DI-CAM-01:DET:Acquire";
        channel.mode = Mode.READ_ONLY;

        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.ENUM);
        pvValue.value = new short[]{1};

        bridge.registerHosted(channel);
        emitter.send(MqttMessage.of(channel.mqttTopic, mapper.writeValueAsBytes(pvValue)));

        await().atMost(5, SECONDS).untilAsserted(
                () -> {
                    PVValue response = adapter.getHosted(channel.pvName);
                    Assertions.assertEquals("Acquire", response.metadata.labels[((short[]) response.value)[0]]);
                });

        pvValue.value = new short[]{0};
        emitter.send(MqttMessage.of(channel.mqttTopic, mapper.writeValueAsBytes(pvValue)));

        await().atMost(5, SECONDS).untilAsserted(
                () -> {
                    PVValue response = adapter.getHosted(channel.pvName);
                    Assertions.assertEquals("Done", response.metadata.labels[((short[]) response.value)[0]]);
                });
    }


    /**
     * Tests getting an external ENUM value through the CA server
     */
    @Test
    public void testExternalChannelGetEnum() throws Exception {

        ChannelAccessCoreTest.TestContext context = new ChannelAccessCoreTest.TestContext();
        context.configure(new DefaultConfiguration("CONTEXT"));

        CAClient caClient = new CAClient(context, adapter);

        Channel channel = new Channel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/external_enum";
        channel.pvName = "remote:pv:enum";
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);
        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.ENUM);
        pvValue.value = new short[]{1};
        pvValue.metadata.labels = new String[]{"Bad", "Good"};
        pvValue.timestamp = Instant.now();
        bridge.putExternal(channel.pvName, pvValue);

        await().atMost(5, SECONDS).untilAsserted(
                () -> {
                    DBR dbr = caClient.get(channel.pvName, DBRType.STRING);
                    Assertions.assertEquals(pvValue.metadata.labels[1], ((DBR_String) dbr.convert(DBRType.STRING)).getStringValue()[0]);
                });
    }

    /**
     * Tests putting an external ENUM value through the CA server
     */
    @Test
    public void testExternalChannelPut() throws Exception {
        class TestContext extends CAJContext {
        }

        TestContext context = new TestContext();
        context.configure(new DefaultConfiguration("CONTEXT"));

        CAClient caClient = new CAClient(context, adapter);

        Channel channel = new Channel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/external_enum";
        channel.pvName = "remote:pv:enum";
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);

        bridge.registerExternal(channel);
        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.ENUM);
        pvValue.value = new short[]{0};
        pvValue.metadata.labels = new String[]{"Bad", "Good"};
        pvValue.timestamp = Instant.now();

        // Some value needs to exist in MQTT already to know its type
        bridge.putExternal(channel.pvName, pvValue);

        caClient.put(channel.pvName, new String[]{"Good"});

        await().atMost(5, SECONDS).untilAsserted(
                () -> {
                    byte[] lastMessage = spy.getLastMessage();
                    Assertions.assertNotNull(lastMessage);
                    Assertions.assertEquals(1, ((short[]) mapper.readValue(lastMessage, PVValue.class).value)[0]);
                });
    }

    @ApplicationScoped
    public static class BrokerSpy {

        private volatile byte[] lastMessage;

        @Incoming("pv/+")
        public CompletionStage<Void> listen(MqttMessage<byte[]> message) {
            this.lastMessage = message.getPayload();

            return message.ack();
        }

        public byte[] getLastMessage() {
            return lastMessage;
        }
    }
}
