package org.excf.epicsmqtt.gateway.adapter.ca;

import gov.aps.jca.CAStatus;
import gov.aps.jca.cas.*;
import gov.aps.jca.dbr.*;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.Dependent;
import org.excf.epicsmqtt.gateway.model.PVValue;

import java.net.InetSocketAddress;

@Dependent
public class CAServer implements Server {

    private final ChannelAccessAdapter adapter;

    public CAServer(ChannelAccessAdapter adapter) {
        super();
        this.adapter = adapter;
    }

    @Override
    public ProcessVariableExistanceCompletion processVariableExistanceTest(String s,
                                                                           InetSocketAddress inetSocketAddress, ProcessVariableExistanceCallback processVariableExistanceCallback)
            throws IllegalArgumentException, IllegalStateException {
        if (adapter.servesChannel(s))
            return ProcessVariableExistanceCompletion.EXISTS_HERE;
        return ProcessVariableExistanceCompletion.DOES_NOT_EXIST_HERE;
    }

    @Override
    public ProcessVariable processVariableAttach(String s, ProcessVariableEventCallback processVariableEventCallback,
                                                 ProcessVariableAttachCallback processVariableAttachCallback)
            throws IllegalArgumentException, IllegalStateException {
        if (adapter.servesChannel(s)) {
            return new ReactivePV(s, adapter.getExternal(s));
        }
        return null;
    }

    private class ReactivePV extends ProcessVariable {
        private final PVValue initialMeta;

        public ReactivePV(String name, PVValue initialMeta) {
            super(name, null);
            this.initialMeta = initialMeta;
        }

        @Override
        public DBRType getType() {
            return initialMeta.getDBRType();
        }

        @Override
        public String[] getEnumLabels() {
            return initialMeta.metadata.labels;
        }

        @Override
        public CAStatus read(DBR dbr, ProcessVariableReadCallback readCallback) {
            adapter.getExternalAsync(name)
                    .emitOn(Infrastructure.getDefaultExecutor())
                    .subscribe().with(
                            pvValue -> {
                                try {
                                    fillDBR(dbr, pvValue);
                                    readCallback.processVariableReadCompleted(CAStatus.NORMAL);
                                } catch (Exception e) {
                                    Log.errorf(e, "Error filling DBR for %s", name);
                                    readCallback.processVariableReadCompleted(CAStatus.DEFUNCT);
                                }
                            },
                            failure -> {
                                Log.errorf(failure, "Async read failed for %s", name);
                                readCallback.processVariableReadCompleted(CAStatus.DEFUNCT);
                            }
                    );
            return null;
        }

        @Override
        public CAStatus write(DBR dbr, ProcessVariableWriteCallback writeCallback) {
            try {
                PVValue newValue = adapter.convertDBRToPVValue(dbr);

                adapter.putExternalAsync(name, newValue)
                        .subscribe().with(
                                success -> {
                                    writeCallback.processVariableWriteCompleted(CAStatus.NORMAL);
                                },
                                failure -> {
                                    Log.errorf(failure, "Async write failed for %s", name);
                                    writeCallback.processVariableWriteCompleted(CAStatus.DEFUNCT);
                                }
                        );

                return null;
            } catch (Exception e) {
                Log.error("Write setup failed", e);
                return CAStatus.DEFUNCT;
            }
        }
    }

    private void fillDBR(DBR dbr, PVValue pvValue) {
        PrimitiveConverter.toPrimitiveArray(pvValue.value, dbr.getValue());

        // Extract Metadata
        if (dbr instanceof GR gr) {
            gr.setUnits(pvValue.metadata.units);
            gr.setUpperDispLimit(pvValue.metadata.upperDisplayLimit);
            gr.setLowerDispLimit(pvValue.metadata.lowerDisplayLimit);
            gr.setUpperAlarmLimit(pvValue.metadata.upperAlarmLimit);
            gr.setUpperWarningLimit(pvValue.metadata.upperWarningLimit);
            gr.setLowerWarningLimit(pvValue.metadata.lowerWarningLimit);
            gr.setLowerAlarmLimit(pvValue.metadata.lowerAlarmLimit);
        }

        // Extract precision
        if (dbr instanceof PRECISION pr) {
            pr.setPrecision((short) (int) pvValue.metadata.precision);
        }

        // Extract control
        if (dbr instanceof CTRL ctrl) {
            ctrl.setLowerCtrlLimit(pvValue.metadata.lowerControlLimit);
            ctrl.setUpperCtrlLimit(pvValue.metadata.upperControlLimit);
        }

        // Extract Status
        if (dbr instanceof STS sts) {
            sts.setStatus(pvValue.getStatus());
            sts.setSeverity(pvValue.getSeverity());
        }

        // Extract Time
        if (dbr instanceof TIME t) {
            t.setTimeStamp(new TimeStamp(pvValue.timestamp.getEpochSecond(), pvValue.timestamp.getNano()));
        }

        if (dbr instanceof LABELS l) {
            l.setLabels(pvValue.metadata.labels);
        }
    }

}
