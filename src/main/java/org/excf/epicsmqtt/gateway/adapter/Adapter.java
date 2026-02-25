package org.excf.epicsmqtt.gateway.adapter;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.bridge.Bridge;
import org.excf.epicsmqtt.gateway.model.PV;
import org.excf.epicsmqtt.gateway.model.PVValue;

public abstract class Adapter {
    @Inject
    protected Bridge bridge;

    public Uni<PVValue> getHosted(String channel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void monitorHosted(String channel){
        throw new UnsupportedOperationException("Not implemented");
    }

    public Uni<Void> putHosted(PV pv) {
        throw new UnsupportedOperationException("Not implemented");
    }


    public Uni<PVValue> getExternalCached(String channel) {
        return bridge.getExternalCached(channel);
    }

    public Uni<PVValue> getExternal(String channel) {
        return bridge.getExternal(channel);
    }

    public Uni<Void> putExternal(PV pv) {
        return bridge.putExternal(pv);
    }

    public Uni<Void> put(PV pv) {
        return bridge.put(pv);
    }
}
