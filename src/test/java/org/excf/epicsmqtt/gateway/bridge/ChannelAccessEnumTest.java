package org.excf.epicsmqtt.gateway.bridge;

import com.cosylab.epics.caj.CAJContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.aps.jca.configuration.DefaultConfiguration;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_String;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.adapter.ca.CAClient;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.config.ExternalChannel;
import org.excf.epicsmqtt.gateway.config.HostedChannel;
import org.excf.epicsmqtt.gateway.config.Mode;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.excf.epicsmqtt.gateway.mqtt.MqttService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@QuarkusTest
public class ChannelAccessEnumTest {
    @Inject
    Bridge bridge;

    @Inject
    ChannelAccessAdapter adapter;

    @Inject
    MqttService mqttService;

    @Inject
    ObjectMapper mapper;

    @Inject
    ChannelAccessCoreTest.BrokerSpy spy;

    /**
     * Tests CA client get and put to local ENUM type channel
     */
    @Test
    public void testHostedChannelEnum() throws Exception {

        HostedChannel channel = new HostedChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/colormode";
        channel.pvName = "BL01T-DI-CAM-01:DET:ColorMode";
        channel.mode = Mode.READ_ONLY;

        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.ENUM);
        pvValue.value = new short[]{1};

        bridge.registerHosted(channel);
        mqttService.publish(channel.mqttTopic, new PV(channel.pvName, pvValue)).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    PVValue response = adapter.getHosted(channel.pvName).await().indefinitely();
                    Assertions.assertEquals("RGB1", response.metadata.labels[((short[]) response.value)[0]]);
                });

        pvValue.value = new short[]{2};
        mqttService.publish(channel.mqttTopic, new PV(channel.pvName, pvValue)).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    PVValue response = adapter.getHosted(channel.pvName).await().indefinitely();
                    Assertions.assertEquals("RGB2", response.metadata.labels[((short[]) response.value)[0]]);
                });

        bridge.removeHosted(channel.pvName);
    }


    /**
     * Tests getting an external ENUM value through the CA server
     */
    @Test
    public void testExternalChannelGetEnum() throws Exception {

        ChannelAccessCoreTest.TestContext context = new ChannelAccessCoreTest.TestContext();
        context.configure(new DefaultConfiguration("CONTEXT"));

        CAClient caClient = new CAClient(context, adapter);

        ExternalChannel channel = new ExternalChannel();
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
        bridge.putExternal(new PV(channel.pvName, pvValue)).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    DBR dbr = caClient.get(channel.pvName, DBRType.STRING).await().indefinitely();
                    Assertions.assertEquals(pvValue.metadata.labels[1], ((DBR_String) dbr.convert(DBRType.STRING)).getStringValue()[0]);
                });

        bridge.removeExternal(channel.pvName);
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

        ExternalChannel channel = new ExternalChannel();
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
        bridge.putExternal(new PV(channel.pvName, pvValue)).await().indefinitely();

        caClient.put(channel.pvName, new String[]{"Good"}).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    byte[] lastMessage = spy.getLastMessage();
                    Assertions.assertNotNull(lastMessage);
                    Assertions.assertEquals(1, ((short[]) mapper.readValue(lastMessage, PV.class).pvValue.value)[0]);
                });

        bridge.removeExternal(channel.pvName);
    }

    @BeforeEach
    public void reset() {
        spy.reset();
    }
}
