package org.excf.epicsmqtt.gateway.adapter.opc;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.milo.opcua.sdk.server.EndpointConfig;
import org.eclipse.milo.opcua.sdk.server.ManagedNamespaceWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.items.DataItem;
import org.eclipse.milo.opcua.sdk.server.items.MonitoredItem;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransport;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransportConfig;
import org.excf.epicsmqtt.gateway.model.PVValue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public class OPCServer {
    private final OPCAdapter adapter;

    private final Thread opcServerThread;

    public OPCServer(OPCAdapter adapter) {
        this.adapter = adapter;

        int port = ConfigProvider.getConfig().getOptionalValue("opc.server.port", Integer.class).orElse(50000);
        OpcUaServerConfig config = OpcUaServerConfig.builder()
                .setExecutor(Executors.newVirtualThreadPerTaskExecutor())
                .setApplicationName(LocalizedText.english("EPICS MQTT GateWay OPC Server"))
                .setApplicationUri("org:excf:epics:opc:server")
                .setEndpoints(Set.of(EndpointConfig.newBuilder()
                        .setHostname("localhost")
                        .setBindAddress("0.0.0.0")
                        .setBindPort(port)
                        .setSecurityPolicy(SecurityPolicy.None)
                        .addTokenPolicies(OpcUaServerConfig.USER_TOKEN_POLICY_ANONYMOUS)
                        .setSecurityMode(MessageSecurityMode.None)
                        .build()
                )).build();

        OpcUaServer server = new OpcUaServer(config, transportProfile -> new OpcTcpServerTransport(OpcTcpServerTransportConfig.newBuilder().build()));

        String namespaceUri = "org:excf:epics";

        CustomNameSpace customNamespace = new CustomNameSpace(server, namespaceUri, adapter);
        customNamespace.startup();

        opcServerThread = new Thread(() -> {
            try {
                server.startup().get();

                final CompletableFuture<Void> future = new CompletableFuture<>();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> future.complete(null)));
                future.get();
            } catch (Exception e) {
                customNamespace.shutdown();
                server.shutdown();
            }
        });

        server.startup();
    }


    static class CustomNameSpace extends ManagedNamespaceWithLifecycle {

        OPCAdapter adapter;

        public CustomNameSpace(OpcUaServer server, String namespaceUri, OPCAdapter adapter) {
            super(server, namespaceUri);
            this.adapter = adapter;
        }

        @Override
        public List<DataValue> read(ReadContext context, Double maxAge, TimestampsToReturn timestamps, List<ReadValueId> readValueIds) {
            return Multi.createFrom().iterable(readValueIds)
                    .onItem().transformToUniAndConcatenate(readValueId -> adapter.getExternalCached(readValueId.getNodeId().toParseableString())
                            .chain(pvValue -> {
                                NodeId nodeId = readValueId.getNodeId();
                                UInteger attributeId = readValueId.getAttributeId();

                                if (attributeId.equals(AttributeId.Value.uid()))
                                    return adapter.getExternal(nodeId.toParseableString()).map(OPCServer::toDataValue);
                                else if (attributeId.equals(AttributeId.NodeClass.uid()))
                                    return Uni.createFrom().item(new DataValue(new Variant(NodeClass.Variable.getValue()), StatusCode.GOOD));
                                else if (attributeId.equals(AttributeId.NodeId.uid()))
                                    return Uni.createFrom().item(new DataValue(new Variant(nodeId), StatusCode.GOOD));
                                else if (attributeId.equals(AttributeId.BrowseName.uid()))
                                    return Uni.createFrom().item(new DataValue(new Variant(new QualifiedName(nodeId.getNamespaceIndex(), (String) nodeId.getIdentifier())), StatusCode.GOOD));
                                else if (attributeId.equals(AttributeId.DisplayName.uid()))
                                    return Uni.createFrom().item(new DataValue(new Variant(LocalizedText.english((String) nodeId.getIdentifier())), StatusCode.GOOD));
                                else if (attributeId.equals(AttributeId.DataType.uid()))
                                    return Uni.createFrom().item(new DataValue(new Variant(OPCServer.opcType(pvValue.value)), StatusCode.GOOD));
                                else if (attributeId.equals(AttributeId.AccessLevel.uid()) || attributeId.equals(AttributeId.UserAccessLevel.uid()))
                                    return Uni.createFrom().item(new DataValue(new Variant(Unsigned.ubyte(1)), StatusCode.GOOD));
                                else if (attributeId.equals(AttributeId.ValueRank.uid()))
                                    return Uni.createFrom().item(new DataValue(new Variant(pvValue.getCount() == 1 ? -1 : 1), StatusCode.GOOD));
                                else if (attributeId.equals(AttributeId.ArrayDimensions.uid()))
                                    return Uni.createFrom().item(new DataValue(new Variant(pvValue.getCount()), StatusCode.GOOD));
                                else if (attributeId.equals(AttributeId.Historizing.uid()))
                                    return Uni.createFrom().item(new DataValue(new Variant(false), StatusCode.GOOD));
                                else
                                    return Uni.createFrom().item(new DataValue(new StatusCode(StatusCodes.Bad_AttributeIdInvalid)));
                            }))
                    .onFailure().recoverWithItem(failure -> new DataValue(new StatusCode(StatusCodes.Bad_NodeIdUnknown)))
                    .collect().asList()
                    .await().indefinitely();
        }

        @Override
        public void onDataItemsCreated(List<DataItem> list) {

        }

        @Override
        public void onDataItemsModified(List<DataItem> list) {

        }

        @Override
        public void onDataItemsDeleted(List<DataItem> list) {

        }

        @Override
        public void onMonitoringModeChanged(List<MonitoredItem> list) {

        }
    }

    private static NodeId opcType(Object value) {
        return switch (value) {
            case int[] ig -> NodeIds.Int32;
            case double[] du -> NodeIds.Double;
            case byte[] by -> NodeIds.Byte;
            case short[] sh -> NodeIds.Int16;
            case float[] fl -> NodeIds.Float;
            case String[] s -> NodeIds.String;
            default -> throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
        };
    }

    private static DataValue toDataValue(PVValue pvValue) {
        Variant data = Variant.of(switch (pvValue.value) {
            case int[] ig -> ig.length == 1 ? ig[0] : ig;
            case double[] du -> du.length == 1 ? du[0] : du;
            case byte[] by -> by.length == 1 ? by[0] : by;
            case short[] sh -> sh.length == 1 ? sh[0] : sh;
            case float[] fl -> fl.length == 1 ? fl[0] : fl;
            case Object[] o -> o.length == 1 ? o[0] : o;
            default -> throw new IllegalStateException("Unexpected value: " + pvValue.value.getClass().getName());
        });

        return new DataValue(data, pvValue.status == 0 ? StatusCode.GOOD : StatusCode.BAD, new DateTime(pvValue.timestamp));
    }

    public void clear() {
        if (opcServerThread != null) {
            try {
                opcServerThread.interrupt();
            } catch (Exception e) {
                Log.warn("OPC server didn't shut down gracefully", e);
            }
        }
    }
}
