package org.excf.epicsmqtt.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class PV {
    public String pvName;
    public PVValue pvValue;
    public boolean monitored = false;

    @JsonIgnore
    // Topic this PV arrived from
    public String topic;

    public PV(){}

    public PV(String pvName, PVValue pvValue) {
        this.pvName = pvName;
        this.pvValue = pvValue;
    }

    public PV(String pvName, PVValue pvValue, boolean monitored) {
        this.pvName = pvName;
        this.pvValue = pvValue;
        this.monitored = monitored;
    }

}
