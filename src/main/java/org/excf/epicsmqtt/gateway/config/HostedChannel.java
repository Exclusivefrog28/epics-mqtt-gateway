package org.excf.epicsmqtt.gateway.config;

public class HostedChannel extends ExternalChannel {
    public boolean monitor = false;
    public String protocol;

    public String getSourceName(){
        return localNames.get(protocol);
    }
}
