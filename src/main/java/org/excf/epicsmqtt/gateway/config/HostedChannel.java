package org.excf.epicsmqtt.gateway.config;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class HostedChannel extends ExternalChannel {
    public boolean monitor = false;
    public String protocol;

    @JsonIgnore
    public String getSourceName(){
        return localNames.get(protocol);
    }
}
