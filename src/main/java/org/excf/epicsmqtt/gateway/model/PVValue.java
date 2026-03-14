package org.excf.epicsmqtt.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
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
    @JsonProperty("status")
    public int status;
    @JsonProperty("severity")
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
    public void setDBRType(DBRType type) {
        this.type = type.getValue();
    }

    @JsonIgnore
    public DBRType getDBRType() {
        return DBRType.forValue(type);
    }

    @JsonIgnore
    public Status getStatus() {
        return Status.forValue(status);
    }

    @JsonIgnore
    public void setStatus(Status status) {
        this.status = status.getValue();
    }

    @JsonIgnore
    public Severity getSeverity() {
        return Severity.forValue(severity);
    }

    @JsonIgnore
    public void setSeverity(Severity severity) {
        this.severity = severity.getValue();
    }

    @JsonIgnore
    public int getCount() {
        return switch (value) {
            case int[] ig -> ig.length;
            case double[] du -> du.length;
            case byte[] by -> by.length;
            case short[] sh -> sh.length;
            case float[] fl -> fl.length;
            case String[] st -> st.length;
            default -> 0;
        };
    }

    @JsonIgnore
    public double getDoubleValue(){
        return switch (value) {
            case int[] ig -> ig[0];
            case double[] du -> du[0];
            case byte[] by -> by[0];
            case short[] sh -> sh[0];
            case float[] fl -> fl[0];
            default -> 0;
        };
    }

    public PVValue() {
    }
}
