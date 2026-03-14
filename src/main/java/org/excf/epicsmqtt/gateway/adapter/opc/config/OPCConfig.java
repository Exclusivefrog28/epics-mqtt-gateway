package org.excf.epicsmqtt.gateway.adapter.opc.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.excf.epicsmqtt.gateway.model.PVMetadata;

import java.util.HashMap;
import java.util.Map;

public class OPCConfig {

    public Map<String, OPCConfigEntry> items = new HashMap<>();

    @JsonIgnore
    public OPCConfig addItem(String alias, OPCConfigEntry item) {
        items.put(alias, item);
        return this;
    }

    public static class OPCConfigEntry {

        public String data;
        public OPCAlarmConfig alarm;
        public PVMetadata meta;

        @JsonIgnore
        public OPCConfigEntry data(String data) {
            this.data = data;
            return this;
        }

        @JsonIgnore
        public OPCConfigEntry alarm(OPCAlarmConfig alarm) {
            this.alarm = alarm;
            return this;
        }

        @JsonIgnore
        public OPCConfigEntry meta(PVMetadata meta) {
            this.meta = meta;
            return this;
        }
    }

    public static class OPCAlarmConfig {
        public String hhsv = "NO_ALARM";
        public String hsv = "NO_ALARM";
        public String lsv = "NO_ALARM";
        public String llsv = "NO_ALARM";

        @JsonIgnore
        public OPCAlarmConfig hhsv(String hhsv) {
            this.hhsv = hhsv;
            return this;
        }

        @JsonIgnore
        public OPCAlarmConfig hsv(String hsv) {
            this.hsv = hsv;
            return this;
        }

        @JsonIgnore
        public OPCAlarmConfig lsv(String lsv) {
            this.lsv = lsv;
            return this;
        }

        @JsonIgnore
        public OPCAlarmConfig llsv(String llsv) {
            this.llsv = llsv;
            return this;
        }

    }
}
