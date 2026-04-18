package org.excf.epicsmqtt.gateway.adapter.opc;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.Severity;
import gov.aps.jca.dbr.Status;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
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
import org.excf.epicsmqtt.gateway.adapter.Adapter;
import org.excf.epicsmqtt.gateway.adapter.opc.config.OPCConfig;
import org.excf.epicsmqtt.gateway.config.yaml.Yaml;
import org.excf.epicsmqtt.gateway.model.PVValue;

import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@ApplicationScoped
@Named("opc")
public class OPCAdapter extends Adapter {
    private OPCClient opcClient;
    private OPCServer opcServer;
    private Map<String, OPCConfig.OPCConfigEntry> opcConfig;

    @Inject
    @Yaml
    ObjectMapper mapper;

    @Override
    public String protocol() {
        return "opc";
    }

    public void setOpcConfig(Map<String, OPCConfig.OPCConfigEntry> opcConfig) {
        this.opcConfig = opcConfig;
    }

    @Override
    public Uni<PVValue> getHosted(String name) {
        if (opcClient == null) return Uni.createFrom().failure(new IllegalArgumentException("OPC client hasn't been initialized"));

        if (opcConfig != null && opcConfig.containsKey(name)) {
            OPCConfig.OPCConfigEntry config = opcConfig.get(name);
            return opcClient.get(config.data)
                    .map(this::convertDataValuetoPVValue)
                    .map(pvValue -> fillAlarm(pvValue, config))
                    .onFailure().invoke(e -> Log.warn("OPC get exception", e));
        }

        return opcClient.get(name)
                .map(this::convertDataValuetoPVValue)
                .onFailure().invoke(e -> Log.warn("OPC get exception", e));
    }

    @Override
    public Multi<PVValue> monitorHosted(String name) {
        if (opcConfig != null && opcConfig.containsKey(name)) {
            OPCConfig.OPCConfigEntry config = opcConfig.get(name);
            return opcClient.monitor(config.data)
                    .map(this::convertDataValuetoPVValue)
                    .map(pvValue -> fillAlarm(pvValue, config))
                    .onFailure().invoke(e -> Log.warn("OPC monitor exception", e));
        }

        return opcClient.monitor(name)
                .map(this::convertDataValuetoPVValue)
                .onFailure().invoke(e -> Log.warn("OPC monitor exception", e));
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
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("opc.yml")) {
            if (is != null) {
                Log.warn("No gateway-config.yml found, skipping configuration.");

                opcConfig = mapper.readValue(is, OPCConfig.class).items;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load opc.yml", e);
        }


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
                                    .setApplicationName(LocalizedText.english("EPICS MQTT GateWay OPC Client"))
                                    .setApplicationUri("org:excf:epics:opc:client")
            ));

        opcServer = new OPCServer(this);
    }

    void shutDown(@Observes ShutdownEvent ev) {
        if (opcClient != null) opcClient.clear();
        if (opcServer != null) opcServer.clear();
    }

    PVValue convertDataValuetoPVValue(DataValue dataValue) {
        Variant variant = dataValue.getValue();
        Object opcValue = variant.getValue();

        PVValue pvValue = new PVValue();
        pvValue.timestamp = dataValue.getSourceTime() != null ? dataValue.getSourceTime().getJavaInstant() : Instant.now();

        switch ((int) dataValue.getStatusCode().getValue()) {
            case 0 -> { // StatusCodes0.Good
                pvValue.setStatus(Status.NO_ALARM);
                pvValue.setSeverity(Severity.NO_ALARM);
            }
            case (int) 0x80310000L -> { // Bad_NoCommunication
                pvValue.setStatus(Status.COMM_ALARM);
                pvValue.setSeverity(Severity.MAJOR_ALARM);
            }
            case (int) 0x80850000L -> { // Bad_RequestTimeout
                pvValue.setStatus(Status.TIMEOUT_ALARM);
                pvValue.setSeverity(Severity.MAJOR_ALARM);
            }
            case (int) 0x803A0000L -> { // Bad_NotReadable
                pvValue.setStatus(Status.READ_ACCESS_ALARM);
                pvValue.setSeverity(Severity.MAJOR_ALARM);
            }
            case (int) 0x803B0000L -> { // Bad_NotWritable
                pvValue.setStatus(Status.WRITE_ACCESS_ALARM);
                pvValue.setSeverity(Severity.MAJOR_ALARM);
            }
            default -> {
                pvValue.setStatus(Status.READ_ALARM);
                pvValue.setSeverity(Severity.INVALID_ALARM);
            }
        }

        if (opcValue == null) return pvValue;

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
            case null, default -> throw new IllegalStateException("Unexpected value: " + opcValue);
        }

        return pvValue;
    }

    DataValue fillDataValue(DataValue dataValue, PVValue pvValue) {
        Object currentValue = dataValue.getValue().getValue();
        if (currentValue == null) throw new IllegalArgumentException("OPC value is null");

        Variant variant = switch (currentValue) {
            case Boolean ignored -> Variant.ofBoolean(Boolean.parseBoolean(((String[]) pvValue.value)[0]));
            case Byte ignored -> Variant.ofSByte(((byte[]) pvValue.value)[0]);
            case UByte ignored -> Variant.ofByte(UByte.valueOf(((byte[]) pvValue.value)[0]));
            case Short ignored -> Variant.ofInt16((short) ((int[]) pvValue.value)[0]);
            case UShort ignored -> Variant.ofUInt16(UShort.valueOf(((int[]) pvValue.value)[0]));
            case Integer ignored -> Variant.ofInt32(((int[]) pvValue.value)[0]);
            case UInteger ignored -> Variant.ofUInt32(UInteger.valueOf(((int[]) pvValue.value)[0]));
            case Long ignored -> Variant.ofInt64(((int[]) pvValue.value)[0]);
            case ULong ignored -> Variant.ofUInt64(ULong.valueOf(((int[]) pvValue.value)[0]));
            case Float ignored -> Variant.ofFloat(((float[]) pvValue.value)[0]);
            case Double ignored -> Variant.ofDouble(((double[]) pvValue.value)[0]);
            case ByteString ignored -> Variant.ofByteString(ByteString.of((byte[]) pvValue.value));
            default -> Variant.ofString(((String[]) pvValue.value)[0]);
        };

        return DataValue.valueOnly(variant);
    }

    PVValue fillAlarm(PVValue pvValue, OPCConfig.OPCConfigEntry config) {
        if (config == null) return pvValue;
        pvValue.metadata = config.meta;
        if (config.alarm == null) return pvValue;
        OPCConfig.OPCAlarmConfig alarmConfig = config.alarm;
        double value = pvValue.getDoubleValue();

        if (!Objects.equals(alarmConfig.hhsv, "NO_ALARM") && value >= config.meta.upperAlarmLimit.doubleValue()) {
            pvValue.setStatus(Status.HIHI_ALARM);
            pvValue.setSeverity(Severity.forName(alarmConfig.hhsv));
        } else if (!Objects.equals(alarmConfig.hsv, "NO_ALARM") && value >= config.meta.upperWarningLimit.doubleValue()) {
            pvValue.setStatus(Status.HIGH_ALARM);
            pvValue.setSeverity(Severity.forName(alarmConfig.hsv));
        } else if (!Objects.equals(alarmConfig.lsv, "NO_ALARM") && value <= config.meta.lowerWarningLimit.doubleValue()) {
            pvValue.setStatus(Status.LOW_ALARM);
            pvValue.setSeverity(Severity.forName(alarmConfig.lsv));
        } else if (!Objects.equals(alarmConfig.llsv, "NO_ALARM") && value <= config.meta.lowerAlarmLimit.doubleValue()) {
            pvValue.setStatus(Status.LOLO_ALARM);
            pvValue.setSeverity(Severity.forName(alarmConfig.llsv));
        }

        return pvValue;
    }
}
