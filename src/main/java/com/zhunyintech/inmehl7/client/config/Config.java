package com.zhunyintech.inmehl7.client.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class Config {

    private static final String EXTERNAL_CONFIG_DIR = "config";
    private static final String EXTERNAL_CONFIG_FILE = "config.properties";
    private final Properties props = new Properties();
    private final Path externalPath;

    public Config() {
        this.externalPath = Paths.get(EXTERNAL_CONFIG_DIR, EXTERNAL_CONFIG_FILE);
        loadClasspathDefaults();
        loadExternalOverrides();
    }

    public String get(String key) {
        if (key == null || key.trim().isEmpty()) {
            return null;
        }
        String sys = System.getProperty(key);
        if (sys != null) {
            return sys;
        }
        String env = System.getenv(key);
        if (env != null) {
            return env;
        }
        String envKey = toEnvKey(key);
        String envMapped = System.getenv(envKey);
        if (envMapped != null) {
            return envMapped;
        }
        return props.getProperty(key);
    }

    public int getInt(String key, int defaultValue) {
        String value = get(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    public void set(String key, String value) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        if (value == null) {
            props.remove(key);
            return;
        }
        props.setProperty(key, value);
    }

    public synchronized void saveExternal() throws IOException {
        Path parent = externalPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
        try (OutputStream out = Files.newOutputStream(externalPath)) {
            props.store(out, "inme-hl7 client local overrides");
        }
    }

    private void loadClasspathDefaults() {
        try (InputStream in = Config.class.getResourceAsStream("/config.properties")) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException ignored) {
        }
    }

    private void loadExternalOverrides() {
        if (!Files.exists(externalPath)) {
            return;
        }
        try (InputStream in = Files.newInputStream(externalPath)) {
            Properties overrides = new Properties();
            overrides.load(in);
            for (String key : overrides.stringPropertyNames()) {
                props.setProperty(key, overrides.getProperty(key));
            }
        } catch (IOException ignored) {
        }
    }

    private String toEnvKey(String key) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c == '.' || c == '-' || c == ' ') {
                sb.append('_');
            } else if (c >= 'a' && c <= 'z') {
                sb.append((char) (c - 32));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}

