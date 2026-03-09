package org.excf.epicsmqtt.gateway.adapter.opc;

import gov.aps.jca.dbr.DBRType;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UByte;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.ULong;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;
import org.eclipse.milo.opcua.stack.core.util.EndpointUtil;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder;
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.model.PVValue;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Optional;

@ApplicationScoped
@Named("opc")
public class OPCAdapter extends Adapter {
    private OPCClient opcClient;

    @Override
    public String protocol() {
        return "opc";
    }

    @Override
    public Uni<PVValue> getHosted(String name) {
        if (opcClient == null) return Uni.createFrom().failure(new IllegalArgumentException("OPC client hasn't been initialized"));

        return opcClient.get(name)
                .map(this::convertDataValuetoPVValue)
                .onFailure().invoke(e -> Log.warn("OPC get exception", e));
    }

    @Override
    public Multi<PVValue> monitorHosted(String name) {
        return null;
    }

    @Override
    public Uni<Void> putHosted(String name, PVValue pvValue) {
        if (opcClient == null) return Uni.createFrom().failure(new IllegalArgumentException("OPC client hasn't been initialized"));

        return opcClient.get(name)
                .map((dataValue -> fillDataValue(dataValue, pvValue)))
                .chain((dataValue -> opcClient.put(name, dataValue)))
                .onFailure().invoke(e -> Log.warn("OPC put exception", e));
    }


    void startup(@Observes StartupEvent ev) throws Exception {
        Optional<String> clientUrl = ConfigProvider.getConfig().getOptionalValue("opc.client.url", String.class);
        Optional<String> clientHost = ConfigProvider.getConfig().getOptionalValue("opc.client.host", String.class);

        if (clientUrl.isPresent())
            opcClient = new OPCClient(OpcUaClient.create(clientUrl.get(),
                    endpoints -> endpoints.stream()
                            .filter(e -> SecurityPolicy.None.getUri().equals(e.getSecurityPolicyUri()))
                            .map(e -> clientHost.map(s -> EndpointUtil.updateUrl(e, s)).orElse(e))
                            .findFirst(),
                    transportConfig -> {
                    },
                    clientConfig ->
                            clientConfig
                                    .setApplicationName(LocalizedText.english("EPICS MQTT Gateway"))
                                    .setApplicationUri("org:excf:org:epics")
            ));

    }

    void shutDown(@Observes ShutdownEvent ev) {
        if (opcClient != null) opcClient.clear();
    }

    private PVValue convertDataValuetoPVValue(DataValue dataValue) {
        Variant variant = dataValue.getValue();
        Object opcValue = variant.getValue();

        PVValue pvValue = new PVValue();
        pvValue.timestamp = Instant.now();

        opcValue = switch (opcValue) {
            case Byte by -> by.byteValue();
            case UByte by -> by.intValue();
            case Short sh -> sh.intValue();
            case UShort sh -> sh.intValue();
            case Integer in -> in.intValue();
            case UInteger in -> in.intValue();
            case Long lo -> lo.intValue();
            case ULong lo -> lo.intValue();
            case Float fl -> fl.floatValue();
            case Double du -> du.doubleValue();
            case String st -> st;
            case ByteString bs -> bs.bytes();
            case LocalizedText lt -> lt.getText();
            default -> opcValue.toString();
        };

        if (opcValue == null) throw new IllegalArgumentException("OPC value is null");

        switch (opcValue) {
            case Byte by -> {
                pvValue.value = new byte[]{by};
                pvValue.setDBRType(DBRType.BYTE);
            }
            case byte[] bs -> {
                pvValue.value = bs;
                pvValue.setDBRType(DBRType.BYTE);
            }
            case Integer in -> {
                pvValue.value = new int[]{in};
                pvValue.setDBRType(DBRType.INT);
            }
            case Float fl -> {
                pvValue.value = new float[]{fl};
                pvValue.setDBRType(DBRType.FLOAT);
            }
            case Double du -> {
                pvValue.value = new double[]{du};
                pvValue.setDBRType(DBRType.DOUBLE);
            }
            case String st -> {
                pvValue.value = new String[]{st};
                pvValue.setDBRType(DBRType.STRING);
            }
            default -> throw new IllegalStateException("Unexpected value: " + opcValue);
        }

        return pvValue;
    }

    private DataValue fillDataValue(DataValue dataValue, PVValue pvValue) {
        Object currentValue = dataValue.getValue().getValue();
        if (currentValue == null) throw new IllegalArgumentException("OPC value is null");

        Variant variant = switch (currentValue) {
            case Boolean un -> Variant.ofBoolean(Boolean.parseBoolean(((String[]) pvValue.value)[0]));
            case Byte un -> Variant.ofSByte(((byte[]) pvValue.value)[0]);
            case UByte un -> Variant.ofByte(UByte.valueOf(((byte[]) pvValue.value)[0]));
            case Short un -> Variant.ofInt16((short) ((int[]) pvValue.value)[0]);
            case UShort un -> Variant.ofUInt16(UShort.valueOf(((int[]) pvValue.value)[0]));
            case Integer un -> Variant.ofInt32(((int[]) pvValue.value)[0]);
            case UInteger un -> Variant.ofUInt32(UInteger.valueOf(((int[]) pvValue.value)[0]));
            case Long un -> Variant.ofInt64(((int[]) pvValue.value)[0]);
            case ULong un -> Variant.ofUInt64(ULong.valueOf(((int[]) pvValue.value)[0]));
            case Float un -> Variant.ofFloat(((float[]) pvValue.value)[0]);
            case Double un -> Variant.ofDouble(((double[]) pvValue.value)[0]);
            case ByteString un -> Variant.ofByteString(ByteString.of((byte[]) pvValue.value));
            default -> Variant.ofString(((String[]) pvValue.value)[0]);
        };

        return DataValue.valueOnly(variant);
    }

}
