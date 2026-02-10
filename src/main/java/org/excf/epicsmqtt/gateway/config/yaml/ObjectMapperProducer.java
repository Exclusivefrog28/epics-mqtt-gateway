package org.excf.epicsmqtt.gateway.config.yaml;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@ApplicationScoped
public class ObjectMapperProducer {

    @Produces
    @Yaml
    public ObjectMapper createYamlMapper() {
        return new ObjectMapper(new YAMLFactory());
    }
}