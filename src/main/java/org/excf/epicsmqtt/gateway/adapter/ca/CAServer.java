package org.excf.epicsmqtt.gateway.adapter.ca;

import gov.aps.jca.CAStatus;
import gov.aps.jca.cas.*;
import gov.aps.jca.dbr.*;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.Dependent;
import org.excf.epicsmqtt.gateway.model.PVValue;

import java.net.InetSocketAddress;

@Dependent
public class CAServer implements Server {

    private ChannelAccessAdapter adapter;

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
            PVValue pvValue = adapter.getExternal(s);

            return new ProcessVariable(s, null) {
                @Override
                public DBRType getType() {
                    return pvValue.getDBRType();
                }

                @Override
                public int getDimensionSize(int dimension) {
                    return switch (pvValue.value) {
                        case int[] ig -> ig.length;
                        case double[] du -> du.length;
                        case byte[] by -> by.length;
                        case short[] sh -> sh.length;
                        case float[] fl -> fl.length;
                        case Object[] o -> o.length;
                        default -> throw new IllegalStateException("Unexpected value: " + pvValue.value);
                    };
                }

                @Override
                public CAStatus read(DBR dbr, ProcessVariableReadCallback processVariableReadCallback) {
                    try {
                        fillDBR(dbr, pvValue);
                    } catch (Exception e) {
                        Log.error("Couldn't serve PV to read", e);
                        return CAStatus.DEFUNCT;
                    }

                    return CAStatus.NORMAL;
                }

                @Override
                public CAStatus write(DBR dbr, ProcessVariableWriteCallback processVariableWriteCallback) {
                    try {
                        adapter.putExternal(s, adapter.convertDBRToPVValue(dbr));
                    } catch (Exception e) {
                        Log.error("Couldn't serve PV to write", e);
                        return CAStatus.DEFUNCT;
                    }

                    return CAStatus.NORMAL;
                }

                @Override
                public String[] getEnumLabels() {
                    return pvValue.metadata.labels;
                }
            };
        }
        return null;
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
