package org.excf.epicsmqtt.gateway.config;

import java.util.Map;

public class ExternalChannel {
    public String alias;
    public String mqttTopic;
    public Map<String, String> localNames;
    public Access access;
}
