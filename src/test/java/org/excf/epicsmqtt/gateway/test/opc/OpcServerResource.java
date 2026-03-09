package org.excf.epicsmqtt.gateway.test.opc;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpcServerResource implements QuarkusTestResourceLifecycleManager {
    private static final String IMAGE = "mcr.microsoft.com/iotedge/opc-plc:latest";

    private GenericContainer<?> opcServer;


    @Override
    public Map<String, String> start() {
        int serverPort = findFreePort();
        String endpointUrl = "opc.tcp://%s:%d".formatted("localhost", serverPort);

        opcServer = new GenericContainer<>(DockerImageName.parse(IMAGE))
                .withReuse(true)
                .withCommand("--autoaccept", "--unsecuretransport",
                        "--portnum=%d".formatted(serverPort)
                )
                .waitingFor(Wait.forListeningPort());

        opcServer.setPortBindings(List.of(
                serverPort + ":" + serverPort + "/tcp"
        ));

        opcServer.start();

        Map<String, String> conf = new HashMap<>();

        conf.put("opc.client.url", endpointUrl);
        conf.put("opc.client.host", "localhost");
        return conf;
    }


    @Override
    public void stop() {
        if (opcServer != null) opcServer.stop();
    }

    private int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Could not find a free port", e);
        }
    }
}
