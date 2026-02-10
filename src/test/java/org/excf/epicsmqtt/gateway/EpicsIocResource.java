package org.excf.epicsmqtt.gateway;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

public class EpicsIocResource implements QuarkusTestResourceLifecycleManager {
    private static final String IMAGE = "ghcr.io/epics-containers/ioc-adsimdetector-demo:2025.11.1";

    private GenericContainer<?> ioc;

    @Override
    public Map<String, String> start() {
        int serverPort = findFreePort();
        int repeaterPort = findFreePort();

        ioc = new GenericContainer<>(DockerImageName.parse(IMAGE))
                .withReuse(true)
                .withEnv("EPICS_CA_SERVER_PORT", String.valueOf(serverPort))
                .withEnv("EPICS_CA_REPEATER_PORT", String.valueOf(repeaterPort))
                .withExposedPorts(serverPort, repeaterPort)
                .waitingFor(Wait.forLogMessage(".*iocRun: All initialization complete.*\\n", 1))
                .withCreateContainerCmdModifier(cmd -> {
                    cmd.withTty(true);
                    cmd.withStdinOpen(true);
                });

        ioc.setPortBindings(java.util.Arrays.asList(
                serverPort + ":" + serverPort + "/tcp",
                serverPort + ":" + serverPort + "/udp",
                repeaterPort + ":" + repeaterPort + "/tcp"
        ));

        ioc.start();

        Map<String, String> conf = new HashMap<>();

        conf.put("ca.client.auto.addr.list", "NO");
        conf.put("ca.client.addr.list", "127.0.0.1:" + serverPort);

        return conf;
    }

    @Override
    public void stop() {
        if (ioc != null) ioc.stop();
    }

    private int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Could not find a free port", e);
        }
    }
}
