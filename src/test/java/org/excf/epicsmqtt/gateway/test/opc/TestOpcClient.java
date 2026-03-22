package org.excf.epicsmqtt.gateway.test.opc;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.excf.epicsmqtt.gateway.adapter.opc.OPCClient;

public class TestOpcClient extends OPCClient {

    public TestOpcClient() throws UaException {
        super(OpcUaClient.create("opc.tcp://localhost:50001",
                endpoints -> endpoints.stream()
                        .filter(e -> SecurityPolicy.None.getUri().equals(e.getSecurityPolicyUri()))
                        .findFirst(),
                transportConfig -> {
                },
                clientConfig ->
                        clientConfig
                                .setApplicationName(LocalizedText.english("EPICS MQTT GateWay OPC Test Client"))
                                .setApplicationUri("org:excf:epics:opc:test:client")
        ));

    }
}
