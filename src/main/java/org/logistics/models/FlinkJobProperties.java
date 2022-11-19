package org.logistics.models;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.nio.file.Files;

public class FlinkJobProperties {

    private static FlinkJobProperties flinkJobProperties;
    PropertyConfig config;

    private FlinkJobProperties() {
        try {
            config = this.setConfig();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PropertyConfig setConfig() throws IOException {
        InputStream inputStream = Files.newInputStream(new File("src/main/resources/config.yml").toPath());
        Yaml yaml = new Yaml(new Constructor(PropertyConfig.class));
        return yaml.load(inputStream);
    }

    public static FlinkJobProperties getInstance() {
        if (flinkJobProperties == null) {
            flinkJobProperties = new FlinkJobProperties();
        }
        return flinkJobProperties;
    }

    public PropertyConfig getConfig() {
        return config;
    }
}
