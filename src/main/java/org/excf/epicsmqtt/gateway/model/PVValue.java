package org.excf.epicsmqtt.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.Severity;
import gov.aps.jca.dbr.Status;

import java.time.Instant;

public class PVValue {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public Object value;

    public int type;
    public Instant timestamp;
    public int status;
    public int severity;
    public PVMetadata metadata = new PVMetadata();

    @JsonIgnore
    public PVValue(Object value, DBRType type, PVMetadata metadata) {
        this.value = value;
        this.type = type.getValue();
        this.metadata = metadata;
        this.timestamp = Instant.now();
    }

    @JsonIgnore
    public void setDBRType(DBRType type){
        this.type = type.getValue();
    }

    @JsonIgnore
    public DBRType getDBRType(){
        return DBRType.forValue(type);
    }

    @JsonIgnore
    public Status getStatus(){
        return Status.forValue(status);
    }

    @JsonIgnore
    public void setStatus(Status status){
        this.status = status.getValue();
    }

    @JsonIgnore
    public Severity getSeverity(){
        return Severity.forValue(severity);
    }

    @JsonIgnore
    public void setSeverity(Severity severity){
        this.severity = severity.getValue();
    }

    @JsonIgnore
    public int getCount(){
        return 0;
    }

    public PVValue() {
    }
}
