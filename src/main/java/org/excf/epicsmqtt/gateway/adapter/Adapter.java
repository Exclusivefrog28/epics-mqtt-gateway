package org.excf.epicsmqtt.gateway.adapter;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.bridge.Bridge;
import org.excf.epicsmqtt.gateway.model.PVValue;

public abstract class Adapter {
    @Inject
    protected Bridge bridge;

    public String protocol() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Uni<PVValue> getHosted(String name) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Multi<PVValue> monitorHosted(String name) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Uni<Void> putHosted(String name, PVValue pvValue) {
        throw new UnsupportedOperationException("Not implemented");
    }


    public Uni<PVValue> getExternalCached(String name) {
        return bridge.getExternalCached(protocol(), name);
    }

    public Uni<PVValue> getExternal(String name) {
        return bridge.getExternal(protocol(), name);
    }

    public Multi<PVValue> addExternalMonitor(String name) {
        return bridge.monitorExternal(protocol(), name);
    }

    public Uni<Void> putExternal(String name, PVValue pvValue) {
        return bridge.putExternal(protocol(), name, pvValue);
    }
}
