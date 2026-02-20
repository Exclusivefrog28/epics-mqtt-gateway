package org.excf.epicsmqtt.gateway.adapter;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.bridge.Bridge;
import org.excf.epicsmqtt.gateway.config.ExternalChannel;
import org.excf.epicsmqtt.gateway.model.PVValue;

import java.util.ArrayList;
import java.util.Collection;

public abstract class Adapter {
    @Inject
    protected Bridge bridge;

    Collection<String> hostedChannels = new ArrayList<>();
    Collection<String> externalChannels = new ArrayList<>();

    public void addHostedChannel(String channel, boolean monitor) {
        hostedChannels.add(channel);
    }

    public void addExternalChannel(String channel) {
        externalChannels.add(channel);
    }

    public void removeHostedChannel(String channel) {
        hostedChannels.remove(channel);
    }

    public void removeExternalChannel(String channel) {
        externalChannels.remove(channel);
    }

    public boolean hostsChannel(String channel) {
        return hostedChannels.contains(channel);
    }

    public boolean servesChannel(String channel) {
        return externalChannels.contains(channel);
    }

    public PVValue getHosted(String channel) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void putHosted(String channel, PVValue value) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    public PVValue getExternal(String channel) {
        return bridge.getExternal(channel);
    }

    public Uni<PVValue> getExternalAsync(String channel) {
        return bridge.getExternalAsync(channel);
    }

    public void putExternal(String channel, PVValue value) {
        bridge.putExternal(channel, value);
    }

    public Uni<Void> putExternalAsync(String channel, PVValue value) {
        return bridge.putExternalAsync(channel, value);
    }

    public void put(String channel, PVValue value) {
        bridge.put(channel, value);
    }


    public ExternalChannel getChannel(String channel) {
        return bridge.getChannel(channel);
    }
}
