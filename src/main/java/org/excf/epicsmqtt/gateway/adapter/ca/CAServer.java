package org.excf.epicsmqtt.gateway.adapter.ca;

import gov.aps.jca.CAStatus;
import gov.aps.jca.Monitor;
import gov.aps.jca.cas.*;
import gov.aps.jca.dbr.*;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.enterprise.context.Dependent;
import org.excf.epicsmqtt.gateway.model.PV;
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

        adapter.getExternalCached(s)
                .subscribe().with(
                        pvValue -> processVariableExistanceCallback.processVariableExistanceTestCompleted(ProcessVariableExistanceCompletion.EXISTS_HERE),
                        failure -> processVariableExistanceCallback.processVariableExistanceTestCompleted(ProcessVariableExistanceCompletion.DOES_NOT_EXIST_HERE)
                );
        return ProcessVariableExistanceCompletion.ASYNC_COMPLETION;
    }

    @Override
    public ProcessVariable processVariableAttach(String s, ProcessVariableEventCallback processVariableEventCallback,
                                                 ProcessVariableAttachCallback processVariableAttachCallback)
            throws IllegalArgumentException, IllegalStateException {
        adapter.getExternalCached(s)
                .subscribe().with(
                        pvValue -> processVariableAttachCallback.processVariableAttachCompleted(new ReactivePV(s, pvValue, processVariableEventCallback)),
                        failure -> processVariableAttachCallback.processVariableAttachCompleted(null)
                );

        return null;
    }

    private class ReactivePV extends ProcessVariable {
        private final PVValue initialMeta;
        private Cancellable subscription;

        public ReactivePV(String name, PVValue initialMeta, ProcessVariableEventCallback processVariableEventCallback) {
            super(name, processVariableEventCallback);
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
        public int getDimensionSize(int dimension) {
            return initialMeta.getCount();
        }

        @Override
        public void interestRegister() {
            super.interestRegister();
            if (subscription == null)
                subscription = adapter.addExternalMonitor(name)
                        .onItem().transform(pv ->
                                fillDBR(DBRType.forValue(pv.pvValue.type).newInstance(pv.pvValue.getCount()), pv.pvValue))
                        .onItem().invoke(dbr -> getEventCallback().postEvent(Monitor.VALUE, dbr))
                        .onFailure().recoverWithItem(th -> null)
                        .subscribe().with(
                                unused -> {
                                },
                                failure -> Log.errorf(failure, "Error monitoring %s", name)
                        );
        }

        @Override
        public void interestDelete() {
            super.interestDelete();
            if (subscription != null) {
                subscription.cancel();
                subscription = null;
            }
        }

        @Override
        public CAStatus read(DBR dbr, ProcessVariableReadCallback readCallback) {
            adapter.getExternal(name)
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

                adapter.putExternal(new PV(name, newValue))
                        .subscribe().with(
                                success -> writeCallback.processVariableWriteCompleted(CAStatus.NORMAL),
                                failure -> {
                                    Log.errorf(failure, "Async write failed for %s", name);
                                    writeCallback.processVariableWriteCompleted(CAStatus.TIMEOUT);
                                }
                        );

                return null;
            } catch (Exception e) {
                Log.error("Write setup failed", e);
                return CAStatus.DEFUNCT;
            }
        }
    }

    private DBR fillDBR(DBR dbr, PVValue pvValue) {
        PrimitiveConverter.toPrimitiveArray(pvValue.value, dbr.getValue());

        // extract metadata
        if (dbr instanceof GR gr) {
            gr.setUnits(pvValue.metadata.units);
            gr.setUpperDispLimit(pvValue.metadata.upperDisplayLimit);
            gr.setLowerDispLimit(pvValue.metadata.lowerDisplayLimit);
            gr.setUpperAlarmLimit(pvValue.metadata.upperAlarmLimit);
            gr.setUpperWarningLimit(pvValue.metadata.upperWarningLimit);
            gr.setLowerWarningLimit(pvValue.metadata.lowerWarningLimit);
            gr.setLowerAlarmLimit(pvValue.metadata.lowerAlarmLimit);
        }

        // extract precision
        if (dbr instanceof PRECISION pr) {
            pr.setPrecision((short) (int) pvValue.metadata.precision);
        }

        // extract control
        if (dbr instanceof CTRL ctrl) {
            ctrl.setLowerCtrlLimit(pvValue.metadata.lowerControlLimit);
            ctrl.setUpperCtrlLimit(pvValue.metadata.upperControlLimit);
        }

        // extract status
        if (dbr instanceof STS sts) {
            sts.setStatus(pvValue.getStatus());
            sts.setSeverity(pvValue.getSeverity());
        }

        // extract time
        if (dbr instanceof TIME t) {
            t.setTimeStamp(new TimeStamp(pvValue.timestamp.getEpochSecond(), pvValue.timestamp.getNano()));
        }

        // extract enum labels
        if (dbr instanceof LABELS l) {
            l.setLabels(pvValue.metadata.labels);
        }

        return dbr;
    }

}
