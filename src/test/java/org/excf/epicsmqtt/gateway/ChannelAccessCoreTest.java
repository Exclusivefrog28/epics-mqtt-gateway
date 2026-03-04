package org.excf.epicsmqtt.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_Double;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.adapter.ca.CAClient;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.bridge.Bridge;
import org.excf.epicsmqtt.gateway.config.ExternalChannel;
import org.excf.epicsmqtt.gateway.config.HostedChannel;
import org.excf.epicsmqtt.gateway.config.Mode;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.excf.epicsmqtt.gateway.mqtt.MqttService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@QuarkusTest
@QuarkusTestResource(EpicsIocResource.class)
@Tag("channel-access")
@Tag("ca-core")
public class ChannelAccessCoreTest {

    @Inject
    Bridge bridge;

    @Inject
    ChannelAccessAdapter adapter;

    @Inject
    MqttService mqttService;

    @Inject
    TestClient testClient;

    @Inject
    ObjectMapper mapper;

    /**
     * Tests CA client get and put to local channel
     */
    @Test
    public void testHostedChannel() {

        HostedChannel channel = new HostedChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/arraycounter";
        channel.localNames = Map.of("ca", "BL01T-DI-CAM-01:DET:ArrayCounter");
        channel.protocol = "ca";
        channel.mode = Mode.READ_WRITE;

        int randomInt = new Random().nextInt(100);

        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.INT);
        pvValue.value = new int[]{randomInt};

        bridge.registerHosted(channel);
        mqttService.publish(channel.mqttTopic + "/PUT", new PV(pvValue), false).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> Assertions.assertEquals(randomInt,
                        ((int[]) adapter.getHosted(channel.getSourceName()).await().indefinitely().value)[0]));

        bridge.removeHosted(channel);
    }

    /**
     * Tests CA client monitoring local channel
     */
    @Test
    public void testHostedChannelMonitor() {
        HostedChannel channel = new HostedChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/uptime";
        channel.localNames = Map.of("ca", "BL01T-EA-TST-02:UPTIME");
        channel.protocol = "ca";
        channel.mode = Mode.READ_ONLY;
        channel.monitor = false;

        AtomicReference<byte[]> lastMessageRef = testClient.subscribe(channel.mqttTopic);
        HashSet<String> receivedValues = new HashSet<>();

        bridge.registerHosted(channel);

        mqttService.publish(channel.mqttTopic + "/MONITOR", "true").await().atMost(Duration.ofSeconds(5));

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    byte[] lastMessage = lastMessageRef.get();
                    Assertions.assertNotNull(lastMessage);
                    String monitorValue = ((String[]) mapper.readValue(lastMessage, PV.class).pvValue.value)[0];
                    Assertions.assertNotNull(monitorValue);
                    receivedValues.add(monitorValue);
                    Assertions.assertTrue(receivedValues.size() > 2);
                });

        bridge.removeHosted(channel);
    }

    /**
     * Tests getting an external value through the CA server
     */
    @Test
    public void testExternalChannelGet() throws Exception {
        CAClient caClient = new CAClient(ChannelAccessTestContext.get());

        ExternalChannel channel = new ExternalChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/external_double";
        channel.localNames = Map.of("ca", "remote:pv");
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);
        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.DOUBLE);
        pvValue.value = new double[]{new Random().nextDouble(0, 100)};
        pvValue.timestamp = Instant.now();

        testClient.addPV(channel.mqttTopic, new PV(channel.localNames, pvValue));

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    DBR dbr = caClient.get("remote:pv").await().indefinitely();
                    Assertions.assertInstanceOf(DBR_Double.class, dbr);
                    Assertions.assertEquals(((double[]) pvValue.value)[0], ((double[]) dbr.getValue())[0], 0.001);
                });

        bridge.removeExternal(channel.mqttTopic);
        testClient.removePV(channel.mqttTopic);
    }

    /**
     * Tests putting an external value through the CA server
     */
    @Test
    public void testExternalChannelPut() throws Exception {
        CAClient caClient = new CAClient(ChannelAccessTestContext.get());

        ExternalChannel channel = new ExternalChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/external_double";
        channel.localNames = Map.of("ca", "remote:pv");
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);

        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.DOUBLE);
        pvValue.value = new double[]{0};
        pvValue.timestamp = Instant.now();

        testClient.addPV(channel.mqttTopic, new PV(channel.localNames, pvValue));
        AtomicReference<byte[]> lastMessageRef = testClient.subscribe(channel.mqttTopic + "/PUT");

        double[] testValue = new double[]{new Random().nextDouble(0, 100)};

        caClient.put("remote:pv", testValue).await().atMost(Duration.ofSeconds(5));

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    byte[] lastMessage = lastMessageRef.get();
                    Assertions.assertNotNull(lastMessage);
                    Assertions.assertEquals(testValue[0], ((double[]) mapper.readValue(lastMessage, PV.class).pvValue.value)[0]);
                });

        bridge.removeExternal(channel.mqttTopic);
        testClient.removePV(channel.mqttTopic);
        testClient.unsubscribe(channel.mqttTopic + "/PUT");
    }

    @Test
    public void testExternalChannelMonitor() throws Exception {
        CAClient caClient = new CAClient(ChannelAccessTestContext.get());

        ExternalChannel channel = new ExternalChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/external_monitored_double";
        channel.localNames = Map.of("ca", "remote:monitored:pv");
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);

        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.DOUBLE);
        pvValue.value = new double[]{0};
        pvValue.timestamp = Instant.now();

        mqttService.publish(channel.mqttTopic, new PV(channel.localNames, pvValue, true)).await().indefinitely();

        AssertSubscriber<Double> subscriber =
                caClient.attachMonitor("remote:monitored:pv")
                        .onItem().transform(dbr -> ((double[]) dbr.getValue())[0])
                        .filter(value -> value != 0.0)
                        .skip().repetitions()
                        .subscribe().withSubscriber(AssertSubscriber.create(2));

        double[] testValue1 = new double[]{new Random().nextDouble(1, 100)};
        pvValue.value = testValue1;
        mqttService.publish(channel.mqttTopic, new PV(channel.localNames, pvValue, true)).await().indefinitely();

        double[] testValue2 = new double[]{new Random().nextDouble(1, 100)};
        pvValue.value = testValue2;
        mqttService.publish(channel.mqttTopic, new PV(channel.localNames, pvValue, true)).await().indefinitely();

        subscriber
                .awaitItems(2)
                .assertItems(testValue1[0], testValue2[0])
                .assertNotTerminated();

        subscriber.cancel();

        bridge.removeExternal(channel.mqttTopic);
    }
}
