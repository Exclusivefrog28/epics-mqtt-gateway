package org.excf.epicsmqtt.gateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.excf.epicsmqtt.gateway.bridge.Bridge;

import java.io.InputStream;

@ApplicationScoped
public class ConfigLoader {

    @Inject
    Bridge bridge;

    void onStart(@Observes StartupEvent ev) {
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("gateway-config.yml")) {
            if (is == null) {
                System.out.println("No gateway-config.yml found, skipping configuration.");
                return;
            }
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            GatewayConfig config = mapper.readValue(is, GatewayConfig.class);

            if (config.hosted != null) {
                for (Channel channel : config.hosted) {
                    bridge.registerHosted(channel);
                }
            }
            if (config.external != null) {
                for (Channel channel : config.external) {
                    bridge.registerExternal(channel);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load gateway-config.yml", e);
        }
    }
}
