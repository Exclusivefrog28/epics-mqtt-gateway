package org.excf.epicsmqtt.gateway.bridge;

import com.cosylab.epics.caj.CAJContext;
import gov.aps.jca.JCALibrary;
import gov.aps.jca.configuration.DefaultConfiguration;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_String;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.excf.epicsmqtt.gateway.adapter.ca.CAClient;
import org.excf.epicsmqtt.gateway.adapter.ca.ChannelAccessAdapter;
import org.excf.epicsmqtt.gateway.config.Channel;
import org.excf.epicsmqtt.gateway.config.Mode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@QuarkusTest
public class BridgeTest {

    @Inject
    Bridge bridge;

    @Inject
    ChannelAccessAdapter adapter;

    @Inject
    @org.eclipse.microprofile.reactive.messaging.Channel("data-out")
    Emitter<String> emitter;

    @Test
    public void testConsumeHostedChannel() throws Exception {
        Channel channel = new Channel();
        channel.alias = "test_alias";
        channel.mqttTopic = "test/topic";
        channel.pvName = "BL01T-DI-CAM-01:DET:ArrayCounter";
        channel.mode = Mode.READ_ONLY;

        int randomInt = new Random().nextInt(100);

        bridge.registerHosted(channel);
        emitter.send(String.valueOf(randomInt));

        await().atMost(5, SECONDS).untilAsserted(
                () -> Assertions.assertEquals(String.valueOf(randomInt), adapter.getHosted(channel.pvName))
        );
    }

    @Test
    public void testServeExternalChannel() throws Exception {
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
        emitter.send("test");

        await().atMost(5, SECONDS).untilAsserted(
                () -> Assertions.assertEquals("test", ((DBR_String) caClient.get(channel.pvName).convert(DBRType.STRING)).getStringValue()[0])
        );
    }
}
