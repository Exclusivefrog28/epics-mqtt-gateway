package org.excf.epicsmqtt.gateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.excf.epicsmqtt.gateway.bridge.Bridge;
import org.excf.epicsmqtt.gateway.config.yaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;

@ApplicationScoped
public class ConfigLoader {

    @Inject
    Bridge bridge;

    @Inject
    @Yaml
    ObjectMapper mapper;

    @ConfigProperty(name = "config.path", defaultValue = "gateway-config.yml")
    String configPath;

    void onStart(@Observes StartupEvent ev) {
        reload();
    }

    public void reload() {
        try (InputStream is = new FileInputStream(configPath)) {
            GatewayConfig config = mapper.readValue(is, GatewayConfig.class);

            if (config.unrestricted){
                bridge.registerAll(config.hosted);
            }else{
                if (config.hosted != null) {
                    for (HostedChannel channel : config.hosted) {
                        bridge.registerHosted(channel);
                    }
                }
                if (config.external != null) {
                    for (ExternalChannel channel : config.external) {
                        bridge.registerExternal(channel);
                    }
                }
            }
        } catch (Exception e) {
            Log.warn("gateway-config.yml could not be loaded, skipping configuration.", e);
        }
    }
}
