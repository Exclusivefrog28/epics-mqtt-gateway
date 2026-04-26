package org.excf.epicsmqtt.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;
import java.util.Objects;

public class PVMetadata {
    public String units = "";
    public String[] labels = new String[]{};
    public Integer precision = 0;
    public Number upperDisplayLimit = 0;
    public Number lowerDisplayLimit = 0;
    public Number upperAlarmLimit = 0;
    public Number lowerAlarmLimit = 0;
    public Number upperWarningLimit = 0;
    public Number lowerWarningLimit = 0;
    public Number upperControlLimit = 0;
    public Number lowerControlLimit = 0;

    @JsonIgnore
    public PVMetadata units(String units) {
        this.units = units;
        return this;
    }

    @JsonIgnore
    public PVMetadata labels(String[] labels) {
        this.labels = labels;
        return this;
    }

    @JsonIgnore
    public PVMetadata precision(Integer precision) {
        this.precision = precision;
        return this;
    }

    @JsonIgnore
    public PVMetadata upperDisplayLimit(Number upperDisplayLimit) {
        this.upperDisplayLimit = upperDisplayLimit;
        return this;
    }

    @JsonIgnore
    public PVMetadata lowerDisplayLimit(Number lowerDisplayLimit) {
        this.lowerDisplayLimit = lowerDisplayLimit;
        return this;
    }

    @JsonIgnore
    public PVMetadata upperAlarmLimit(Number upperAlarmLimit) {
        this.upperAlarmLimit = upperAlarmLimit;
        return this;
    }

    @JsonIgnore
    public PVMetadata lowerAlarmLimit(Number lowerAlarmLimit) {
        this.lowerAlarmLimit = lowerAlarmLimit;
        return this;
    }

    @JsonIgnore
    public PVMetadata upperWarningLimit(Number upperWarningLimit) {
        this.upperWarningLimit = upperWarningLimit;
        return this;
    }

    @JsonIgnore
    public PVMetadata lowerWarningLimit(Number lowerWarningLimit) {
        this.lowerWarningLimit = lowerWarningLimit;
        return this;
    }

    @JsonIgnore
    public PVMetadata upperControlLimit(Number upperControlLimit) {
        this.upperControlLimit = upperControlLimit;
        return this;
    }

    @JsonIgnore
    public PVMetadata lowerControlLimit(Number lowerControlLimit) {
        this.lowerControlLimit = lowerControlLimit;
        return this;
    }

    @JsonIgnore
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PVMetadata that)) return false;
        return Objects.equals(units, that.units) && Objects.deepEquals(labels, that.labels) && Objects.equals(precision, that.precision) && Objects.equals(upperDisplayLimit, that.upperDisplayLimit) && Objects.equals(lowerDisplayLimit, that.lowerDisplayLimit) && Objects.equals(upperAlarmLimit, that.upperAlarmLimit) && Objects.equals(lowerAlarmLimit, that.lowerAlarmLimit) && Objects.equals(upperWarningLimit, that.upperWarningLimit) && Objects.equals(lowerWarningLimit, that.lowerWarningLimit) && Objects.equals(upperControlLimit, that.upperControlLimit) && Objects.equals(lowerControlLimit, that.lowerControlLimit);
    }

    @JsonIgnore
    @Override
    public int hashCode() {
        return Objects.hash(units, Arrays.hashCode(labels), precision, upperDisplayLimit, lowerDisplayLimit, upperAlarmLimit, lowerAlarmLimit, upperWarningLimit, lowerWarningLimit, upperControlLimit, lowerControlLimit);
    }

}
