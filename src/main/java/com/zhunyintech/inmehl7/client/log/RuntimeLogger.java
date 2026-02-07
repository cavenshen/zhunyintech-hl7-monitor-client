package com.zhunyintech.inmehl7.client.log;

import com.zhunyintech.inmehl7.client.config.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class RuntimeLogger {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final Logger logger;
    private Consumer<String> sink;

    public RuntimeLogger(Config config) {
        this.logger = Logger.getLogger("inme-hl7-client");
        this.logger.setUseParentHandlers(false);
        setupFileHandler(config);
    }

    public void bindSink(Consumer<String> sink) {
        this.sink = sink;
    }

    public void info(String message) {
        log(Level.INFO, message, null);
    }

    public void warn(String message) {
        log(Level.WARNING, message, null);
    }

    public void error(String message, Throwable throwable) {
        log(Level.SEVERE, message, throwable);
    }

    private void log(Level level, String message, Throwable throwable) {
        String line = String.format("%s [%s] %s", TS.format(LocalDateTime.now()), level.getName(), message);
        logger.log(level, line, throwable);
        if (sink != null) {
            sink.accept(line);
        }
    }

    private void setupFileHandler(Config config) {
        String dir = config.get("logDir");
        if (dir == null || dir.trim().isEmpty()) {
            dir = "./logs";
        }
        Path logDir = Paths.get(dir);
        try {
            if (!Files.exists(logDir)) {
                Files.createDirectories(logDir);
            }
            String fileName = "inmehl7-runtime-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE) + ".log";
            Path filePath = logDir.resolve(fileName);
            FileHandler handler = new FileHandler(filePath.toString(), true);
            handler.setFormatter(new Formatter() {
                @Override
                public String format(LogRecord record) {
                    return record.getMessage() + System.lineSeparator();
                }
            });
            logger.addHandler(handler);
        } catch (IOException ignored) {
        }
    }
}

