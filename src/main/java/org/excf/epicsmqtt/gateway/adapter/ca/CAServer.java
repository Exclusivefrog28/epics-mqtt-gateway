package org.excf.epicsmqtt.gateway.adapter.ca;

import gov.aps.jca.CAException;
import gov.aps.jca.CAStatus;
import gov.aps.jca.CAStatusException;
import gov.aps.jca.cas.*;
import gov.aps.jca.dbr.*;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import java.net.InetSocketAddress;

@Dependent
public class CAServer implements Server {

    private ChannelAccessAdapter adapter;

    public CAServer(ChannelAccessAdapter adapter) {
        super();
        this.adapter = adapter;
    }

    @Override
    public ProcessVariableExistanceCompletion processVariableExistanceTest(String s, InetSocketAddress inetSocketAddress, ProcessVariableExistanceCallback processVariableExistanceCallback) throws CAException, IllegalArgumentException, IllegalStateException {
        if (adapter.servesChannel(s)) return ProcessVariableExistanceCompletion.EXISTS_HERE;
        return ProcessVariableExistanceCompletion.DOES_NOT_EXIST_HERE;
    }

    @Override
    public ProcessVariable processVariableAttach(String s, ProcessVariableEventCallback processVariableEventCallback, ProcessVariableAttachCallback processVariableAttachCallback) throws CAStatusException, IllegalArgumentException, IllegalStateException {
        if (adapter.servesChannel(s)) return new ProcessVariable(s, null) {
            @Override
            public DBRType getType() {
                return DBRType.STRING;
            }

            @Override
            public CAStatus read(DBR dbr, ProcessVariableReadCallback processVariableReadCallback) {
                if (dbr instanceof DBR_TIME_String) {
                    DBR_TIME_String dbrString = (DBR_TIME_String) dbr;
                    dbrString.getStringValue()[0] = adapter.getExternal(s);
                    dbrString.setTimeStamp(new gov.aps.jca.dbr.TimeStamp());
                    dbrString.setStatus(Status.NO_ALARM);
                    dbrString.setSeverity(Severity.NO_ALARM);
                } else {
                    ((DBR_String) dbr).getStringValue()[0] = adapter.getExternal(s);
                }

                return CAStatus.NORMAL;
            }

            @Override
            public CAStatus write(DBR dbr, ProcessVariableWriteCallback processVariableWriteCallback) {
                adapter.putExternal(s, (((DBR_String) dbr)).getStringValue()[0]);
                return null;
            }
        };
        return null;
    }


}
