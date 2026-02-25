package org.excf.epicsmqtt.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class PV {
    public String pvName;
    public PVValue pvValue;

    @JsonIgnore
    // Topic this PV arrived from
    public String topic;

    public PV(String pvName, PVValue pvValue) {
        this.pvName = pvName;
        this.pvValue = pvValue;
    }

}
