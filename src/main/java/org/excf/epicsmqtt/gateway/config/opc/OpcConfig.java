package org.excf.epicsmqtt.gateway.config.opc;

import org.excf.epicsmqtt.gateway.model.PVMetadata;

import java.util.List;

public class OpcConfig {
    public List<OpcPV> PVs;

    public static class OpcPV{
        public String alias;
        public String data;
        public String alarm;

        public PVMetadata meta;
    }
}
