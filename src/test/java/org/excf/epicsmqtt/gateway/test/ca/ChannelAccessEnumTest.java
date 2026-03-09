package org.excf.epicsmqtt.gateway.test.ca;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_String;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.adapter.ca.CAClient;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.bridge.Bridge;
import org.excf.epicsmqtt.gateway.config.ExternalChannel;
import org.excf.epicsmqtt.gateway.config.HostedChannel;
import org.excf.epicsmqtt.gateway.config.Mode;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.excf.epicsmqtt.gateway.mqtt.MQTTAdapter;
import org.excf.epicsmqtt.gateway.test.TestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@QuarkusTest
@QuarkusTestResource(value = EpicsIocResource.class, restrictToAnnotatedClass = true)
@Tag("channel-access")
@Tag("ca-enum")
public class ChannelAccessEnumTest {
    @Inject
    Bridge bridge;

    @Inject
    ChannelAccessAdapter adapter;

    @Inject
    MQTTAdapter mqttAdapter;

    @Inject
    ObjectMapper mapper;

    @Inject
    TestClient testClient;

    /**
     * Tests CA client get and put to local ENUM type channel
     */
    @Test
    public void testHostedChannelEnum() {

        HostedChannel channel = new HostedChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/colormode";
        channel.localNames = Map.of("ca", "BL01T-DI-CAM-01:DET:ColorMode");
        channel.protocol = "ca";
        channel.mode = Mode.READ_ONLY;

        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.ENUM);
        pvValue.value = new short[]{1};

        bridge.registerHosted(channel);
        mqttAdapter.publishPV(channel.mqttTopic + "/PUT", new PV(pvValue), false).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    PVValue response = adapter.getHosted(channel.getSourceName()).await().indefinitely();
                    Assertions.assertEquals("RGB1", response.metadata.labels[((short[]) response.value)[0]]);
                });

        pvValue.value = new short[]{2};
        mqttAdapter.publishPV(channel.mqttTopic + "/PUT", new PV(channel.localNames, pvValue), false).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    PVValue response = adapter.getHosted(channel.getSourceName()).await().indefinitely();
                    Assertions.assertEquals("RGB2", response.metadata.labels[((short[]) response.value)[0]]);
                });

        bridge.removeHosted(channel.mqttTopic);
    }


    /**
     * Tests getting an external ENUM value through the CA server
     */
    @Test
    public void testExternalChannelGetEnum() throws Exception {
        CAClient caClient = new CAClient(ChannelAccessTestContext.get());

        ExternalChannel channel = new ExternalChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/external_enum";
        channel.localNames = Map.of("ca", "remote:pv:enum");
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);
        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.ENUM);
        pvValue.value = new short[]{1};
        pvValue.metadata.labels = new String[]{"Bad", "Good"};
        pvValue.timestamp = Instant.now();

        testClient.addPV(channel.mqttTopic, new PV(channel.localNames, pvValue));

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    DBR dbr = caClient.get("remote:pv:enum").await().indefinitely();
                    Assertions.assertEquals(pvValue.metadata.labels[1], ((DBR_String) dbr.convert(DBRType.STRING)).getStringValue()[0]);
                });

        bridge.removeExternal(channel.mqttTopic);
        testClient.removePV(channel.mqttTopic);
    }

    /**
     * Tests putting an external ENUM value through the CA server
     */
    @Test
    public void testExternalChannelPutEnum() throws Exception {
        CAClient caClient = new CAClient(ChannelAccessTestContext.get());

        ExternalChannel channel = new ExternalChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/external_enum";
        channel.localNames = Map.of("ca", "remote:pv:enum");
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);

        bridge.registerExternal(channel);
        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.ENUM);
        pvValue.value = new short[]{0};
        pvValue.metadata.labels = new String[]{"Bad", "Good"};
        pvValue.timestamp = Instant.now();

        testClient.addPV(channel.mqttTopic, new PV(channel.localNames, pvValue));
        AtomicReference<byte[]> lastMessageRef = testClient.subscribe(channel.mqttTopic + "/PUT");

        caClient.put("remote:pv:enum", new String[]{"Good"}).await().atMost(Duration.ofSeconds(5));

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    byte[] lastMessage = lastMessageRef.get();
                    Assertions.assertNotNull(lastMessage);
                    Assertions.assertEquals(1, ((short[]) mapper.readValue(lastMessage, PV.class).pvValue.value)[0]);
                });

        bridge.removeExternal(channel.mqttTopic);
        testClient.removePV(channel.mqttTopic);
        testClient.unsubscribe(channel.mqttTopic + "/PUT");
    }
}
