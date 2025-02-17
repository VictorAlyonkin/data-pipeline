package org.petrarka.config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class Config {
    private static final String FILE_NAME_ALL_PROPERTIES = "application.properties";

    public static Properties getProperties() {
        Properties props = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream inputStream = loader.getResourceAsStream(FILE_NAME_ALL_PROPERTIES)) {
           props.load(inputStream);
            return props;
        } catch (IOException exception) {
            log.error("Exception read application.properties", exception);
            throw new RuntimeException(exception);
        }
    }
}
