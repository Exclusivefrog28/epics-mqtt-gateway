package org.excf.epicsmqtt.gateway.bridge;

import com.cosylab.epics.caj.CAJContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.aps.jca.configuration.DefaultConfiguration;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_Double;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.excf.epicsmqtt.gateway.EpicsIocResource;
import org.excf.epicsmqtt.gateway.adapter.ca.CAClient;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.config.Channel;
import org.excf.epicsmqtt.gateway.config.Mode;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Random;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@QuarkusTest
@QuarkusTestResource(EpicsIocResource.class)
public class ChannelAccessBridgeTest {

    @Inject
    Bridge bridge;

    @Inject
    ChannelAccessAdapter adapter;

    @Inject
    @org.eclipse.microprofile.reactive.messaging.Channel("data-out")
    Emitter<String> emitter;

    @Inject
    ObjectMapper mapper;

    @Inject
    BrokerSpy spy;

    static class TestContext extends CAJContext {
    }

    /**
     * Tests CA client get and put to local channel
     */
    @Test
    public void testHostedChannel() throws Exception {

        Channel channel = new Channel();
        channel.alias = "test_alias";
        channel.mqttTopic = "test/topic";
        channel.pvName = "BL01T-DI-CAM-01:DET:ArrayCounter";
        channel.mode = Mode.READ_ONLY;

        int randomInt = new Random().nextInt(100);

        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.INT);
        pvValue.value = new int[]{randomInt};

        bridge.registerHosted(channel);
        emitter.send(mapper.writeValueAsString(pvValue));

        await().atMost(5, SECONDS).untilAsserted(
                () -> Assertions.assertEquals(randomInt,
                        ((int[]) adapter.getHosted(channel.pvName).value)[0]));
    }

    /**
     * Tests getting an external value through the CA server
     */
    @Test
    public void testExternalChannelGet() throws Exception {

        TestContext context = new TestContext();
        context.configure(new DefaultConfiguration("CONTEXT"));

        CAClient caClient = new CAClient(context);

        Channel channel = new Channel();
        channel.alias = "test_alias";
        channel.mqttTopic = "test/topic";
        channel.pvName = "remote:pv";
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);
        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.DOUBLE);
        pvValue.value = new double[]{new Random().nextDouble(0, 100)};
        pvValue.timestamp = Instant.now();
        bridge.put(channel.pvName, pvValue);

        await().atMost(5, SECONDS).untilAsserted(
                () -> {
                    DBR dbr = caClient.get(channel.pvName);
                    Assertions.assertInstanceOf(DBR_Double.class, dbr);
                    Assertions.assertEquals(((double[]) pvValue.value)[0], ((double[]) dbr.getValue())[0], 0.001);
                });
    }

    /**
     * Tests putting an external value through the CA server
     */
    @Test
    public void testExternalChannelPut() throws Exception {
        class TestContext extends CAJContext {
        }

        TestContext context = new TestContext();
        context.configure(new DefaultConfiguration("CONTEXT"));

        CAClient caClient = new CAClient(context);

        Channel channel = new Channel();
        channel.alias = "test_alias";
        channel.mqttTopic = "test/topic";
        channel.pvName = "remote:pv";
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);

        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.DOUBLE);
        pvValue.value = new double[]{0};
        pvValue.timestamp = Instant.now();

        // Some value needs to exist in MQTT already to know its type
        bridge.put(channel.pvName, pvValue);

        double[] testValue = new double[]{new Random().nextDouble(0, 100)};

        caClient.put(channel.pvName, testValue);

        await().atMost(5, SECONDS).untilAsserted(
                () -> {
                    byte[] lastMessage = spy.getLastMessage();
                    Assertions.assertNotNull(lastMessage);
                    Assertions.assertEquals(testValue[0], ((double[]) mapper.readValue(lastMessage, PVValue.class).value)[0]);
                });
    }

    @ApplicationScoped
    public static class BrokerSpy {

        private volatile byte[] lastMessage;

        @Incoming("test/topic")
        public void listen(byte[] payload) {
            this.lastMessage = payload;
        }

        public byte[] getLastMessage() {
            return lastMessage;
        }
    }
}
