package org.excf.epicsmqtt.gateway.test.opc;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.Severity;
import gov.aps.jca.dbr.Status;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.excf.epicsmqtt.gateway.adapter.ca.CAClient;
import org.excf.epicsmqtt.gateway.adapter.opc.OPCAdapter;
import org.excf.epicsmqtt.gateway.adapter.opc.OPCClient;
import org.excf.epicsmqtt.gateway.adapter.opc.config.OPCConfig;
import org.excf.epicsmqtt.gateway.bridge.Bridge;
import org.excf.epicsmqtt.gateway.config.ExternalChannel;
import org.excf.epicsmqtt.gateway.config.HostedChannel;
import org.excf.epicsmqtt.gateway.config.Mode;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.model.PVMetadata;
import org.excf.epicsmqtt.gateway.model.PVValue;
import org.excf.epicsmqtt.gateway.mqtt.MQTTAdapter;
import org.excf.epicsmqtt.gateway.test.TestClient;
import org.excf.epicsmqtt.gateway.test.ca.ChannelAccessTestContext;
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

        AtomicReference<byte[]> lastMessageRef = testClient.subscribe(channel.mqttTopic);
        mqttAdapter.publishBoolean(channel.mqttTopic + "/GET", true).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(() -> {
            byte[] lastMessage = lastMessageRef.get();
            Assertions.assertNotNull(lastMessage);
            Assertions.assertDoesNotThrow(() -> {
                float value = ((float[]) mapper.readValue(lastMessage, PV.class).pvValue.value)[0];
            });
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
        testClient.unsubscribe(channel.mqttTopic);
    }

    @Test
    public void testHostedChannelMonitor() throws Exception {

        HostedChannel channel = new HostedChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/opc-monitored";
        channel.localNames = Map.of("opc", "ns=3;s=RandomUnsignedInt32");
        channel.protocol = "opc";
        channel.mode = Mode.READ_WRITE;

        AtomicReference<byte[]> lastMessageRef = testClient.subscribe(channel.mqttTopic);
        HashSet<Integer> receivedValues = new HashSet<>();

        bridge.registerHosted(channel);

        mqttAdapter.publishBoolean(channel.mqttTopic + "/MONITOR", true).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    byte[] lastMessage = lastMessageRef.get();
                    Assertions.assertNotNull(lastMessage);
                    int monitorValue = ((int[]) mapper.readValue(lastMessage, PV.class).pvValue.value)[0];
                    receivedValues.add(monitorValue);
                    Assertions.assertTrue(receivedValues.size() > 2);
                });

        bridge.removeHosted(channel.mqttTopic);
        testClient.unsubscribe(channel.mqttTopic);
    }

    @Test
    public void testHostedAlarm() {
        HostedChannel channel = new HostedChannel();
        channel.alias = "temperature";
        channel.mqttTopic = "pv/opc-temperature";
        channel.localNames = Map.of("opc", "temperature");
        channel.protocol = "opc";
        channel.mode = Mode.READ_WRITE;

        bridge.registerHosted(channel);

        adapter.setOpcConfig(new OPCConfig().addItem("temperature", new OPCConfig.OPCConfigEntry()
                .data("ns=4;i=6210")
                .alarm(new OPCConfig.OPCAlarmConfig()
                        .hhsv("MAJOR_ALARM")
                        .hsv("MINOR_ALARM")
                        .lsv("MINOR_ALARM")
                        .llsv("MAJOR_ALARM")
                ).meta(new PVMetadata()
                        .lowerAlarmLimit(-10)
                        .lowerWarningLimit(0)
                        .upperWarningLimit(5)
                        .upperAlarmLimit(20)
                )
        ).items);

        AtomicReference<byte[]> lastMessageRef = testClient.subscribe(channel.mqttTopic);
        mqttAdapter.publishBoolean(channel.mqttTopic + "/GET", true).await().indefinitely();

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(() -> {
            byte[] lastMessage = lastMessageRef.get();
            Assertions.assertNotNull(lastMessage);
            PV pv = mapper.readValue(lastMessage, PV.class);

            Assertions.assertEquals(Status.HIGH_ALARM.getValue(), pv.pvValue.status);
            Assertions.assertEquals(Severity.MINOR_ALARM.getValue(), pv.pvValue.severity);
        });

        bridge.removeHosted(channel.mqttTopic);
        testClient.unsubscribe(channel.mqttTopic);
    }

    @Test
    public void testExternalChannelGet() throws Exception {
        OPCClient opcClient = new TestOpcClient();

        ExternalChannel channel = new ExternalChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/external_opc";
        channel.localNames = Map.of("opc", "ns=2;s=RemoteOPCNode");
        channel.mode = Mode.READ_ONLY;

        bridge.registerExternal(channel);
        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.DOUBLE);
        pvValue.value = new double[]{new Random().nextDouble(0, 100)};
        pvValue.timestamp = Instant.now();

        testClient.addPV(channel.mqttTopic, new PV(channel.localNames, pvValue));

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    DataValue dataValue = opcClient.get("ns=2;s=RemoteOPCNode")
                            .onFailure().invoke(e -> Log.warn("OPC test client exception", e))
                            .await().atMost(Duration.ofSeconds(1));
                    Object value = dataValue.getValue().getValue();
                    Assertions.assertNotNull(value);
                    Assertions.assertEquals(((double[]) pvValue.value)[0], (double) value, 0.001);
                });

        bridge.removeExternal(channel.mqttTopic);
        testClient.removePV(channel.mqttTopic);
        opcClient.clear();
    }

    @Test
    public void testExternalChannelPut() throws Exception {
        OPCClient opcClient = new TestOpcClient();

        ExternalChannel channel = new ExternalChannel();
        channel.alias = "test_alias";
        channel.mqttTopic = "pv/external_opc";
        channel.localNames = Map.of("opc", "ns=2;s=RemoteOPCNode");
        channel.mode = Mode.READ_WRITE;

        bridge.registerExternal(channel);

        PVValue pvValue = new PVValue();
        pvValue.setDBRType(DBRType.DOUBLE);
        pvValue.value = new double[]{0};
        pvValue.timestamp = Instant.now();

        testClient.addPV(channel.mqttTopic, new PV(channel.localNames, pvValue));
        AtomicReference<byte[]> lastMessageRef = testClient.subscribe(channel.mqttTopic + "/PUT");

        double testValue = new Random().nextDouble(0, 100);

        opcClient.put(channel.localNames.get("opc"), DataValue.valueOnly(Variant.of(testValue))).await().atMost(Duration.ofSeconds(5));

        await().atMost(5, SECONDS).ignoreExceptions().untilAsserted(
                () -> {
                    byte[] lastMessage = lastMessageRef.get();
                    Assertions.assertNotNull(lastMessage);
                    Assertions.assertEquals(testValue, ((double[]) mapper.readValue(lastMessage, PV.class).pvValue.value)[0]);
                });

        bridge.removeExternal(channel.mqttTopic);
        testClient.removePV(channel.mqttTopic);
        testClient.unsubscribe(channel.mqttTopic + "/PUT");
        opcClient.clear();
    }
}