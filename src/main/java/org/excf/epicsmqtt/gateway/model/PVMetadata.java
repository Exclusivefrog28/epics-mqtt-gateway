package org.excf.epicsmqtt.gateway.model;

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
}
