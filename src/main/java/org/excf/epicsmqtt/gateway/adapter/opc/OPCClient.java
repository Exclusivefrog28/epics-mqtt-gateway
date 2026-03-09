package org.excf.epicsmqtt.gateway.adapter.opc;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.nodes.UaVariableNode;
import org.eclipse.milo.opcua.stack.core.OpcUaDataType;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.structured.WriteValue;

import java.util.List;

public class OPCClient {

    OpcUaClient client;

    public OPCClient(OpcUaClient client) {
        this.client = client;
        try {
            client.connect();
        } catch (UaException e) {
            Log.info("OPC client failed to connect");
            throw new RuntimeException(e);
        }
    }

    public Uni<DataValue> get(String nodeId) {
        try {
            return Uni.createFrom().completionStage(client.getAddressSpace().getVariableNodeAsync(NodeId.parse(nodeId)))
                    .map(Unchecked.function(UaVariableNode::readValue));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> put(String nodeId, DataValue value) {
        try {
            return Uni.createFrom().completionStage(client.writeValuesAsync(List.of(NodeId.parse(nodeId)), List.of(value)))
                    .chain(list -> {
                        if (list.getFirst().isGood()) return Uni.createFrom().voidItem();
                        else return Uni.createFrom().failure(new RuntimeException("OPC write failed"));
                    });
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    public void clear() {
        if (client != null) {
            try {
                client.disconnect();
            } catch (UaException e) {
                Log.warn("OPC client didn't shut down gracefully");
            }
        }
    }


}
