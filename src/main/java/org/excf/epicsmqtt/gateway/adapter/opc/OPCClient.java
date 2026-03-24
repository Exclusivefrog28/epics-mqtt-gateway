package org.excf.epicsmqtt.gateway.adapter.opc;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.unchecked.Unchecked;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class OPCClient {

    OpcUaClient client;

    private OpcUaSubscription subscription;

    public OPCClient(OpcUaClient client) {
        this.client = client;
        try {
            client.connectAsync().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
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

    public Multi<DataValue> monitor(String nodeId) {
        return Multi.createFrom().emitter(emitter -> {
            Infrastructure.getDefaultWorkerPool().execute(() -> {
                try {
                    if (subscription == null) {
                        synchronized (this) {
                            if (subscription == null) {
                                (subscription = new OpcUaSubscription(client)).create(); // Blocking
                            }
                        }
                    }

                    OpcUaMonitoredItem monitoredItem = OpcUaMonitoredItem.newDataItem(NodeId.parse(nodeId));
                    monitoredItem.setDataValueListener((item, value) -> emitter.emit(value));

                    subscription.addMonitoredItem(monitoredItem);

                    subscription.synchronizeMonitoredItems();

                    emitter.onTermination(() -> {

                        Infrastructure.getDefaultWorkerPool().execute(() -> {
                            try {
                                subscription.removeMonitoredItem(monitoredItem);
                                subscription.synchronizeMonitoredItems(); // Blocking
                            } catch (Exception e) {
                                Log.warnf(e, "Couldn't remove OPC monitoredItem for %s", nodeId);
                            }
                        });
                    });

                } catch (Exception e) {
                    emitter.fail(e);
                }
            });
        });
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
                if (subscription != null) {
                    subscription.delete();
                    subscription = null;
                }
                client.disconnect();
            } catch (UaException e) {
                Log.warn("OPC client didn't shut down gracefully", e);
            }
        }
    }


}
