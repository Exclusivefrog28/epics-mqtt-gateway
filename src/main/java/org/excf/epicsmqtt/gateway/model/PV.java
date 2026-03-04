package org.excf.epicsmqtt.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public class PV {
    public Map<String, String> localNames;
    public PVValue pvValue;
    public boolean monitored = false;

    // Topic this PV arrived from
    @JsonIgnore
    public String topic;

    public PV(){}

    public PV(PVValue pvValue) {
        this.pvValue = pvValue;
    }

    public PV(Map<String, String> localNames, PVValue pvValue) {
        this.localNames = localNames;
        this.pvValue = pvValue;
    }

    public PV(Map<String, String> localNames, PVValue pvValue, boolean monitored) {
        this.localNames = localNames;
        this.pvValue = pvValue;
        this.monitored = monitored;
    }

}
