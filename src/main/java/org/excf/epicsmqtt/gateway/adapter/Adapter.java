package org.excf.epicsmqtt.gateway.adapter;

import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.bridge.Bridge;

import java.util.ArrayList;
import java.util.Collection;

public abstract class Adapter {
    @Inject
    protected Bridge bridge;

    Collection<String> hostedChannels = new ArrayList<>();
    Collection<String> externalChannels = new ArrayList<>();

    public void addHostedChannel(String channel) {
        hostedChannels.add(channel);
    }

    public void addExternalChannel(String channel) {
        externalChannels.add(channel);
    }

    public boolean hostsChannel(String channel) {
        return hostedChannels.contains(channel);
    }

    public boolean servesChannel(String channel) {
        return externalChannels.contains(channel);
    }

    public String getHosted(String channel) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void putHosted(String channel, String value) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    public String getExternal(String channel) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void putExternal(String channel, String value) {
        throw new UnsupportedOperationException("Not implemented");
    }

}
