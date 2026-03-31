package com.zhunyintech.inmehl7.client.hl7;

import com.zhunyintech.inmehl7.client.log.RuntimeLogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class ListenerManager {

    private final RuntimeLogger logger;
    private final MllpServer.MessageHandler messageHandler;
    private final Map<Integer, MllpServer> servers = new LinkedHashMap<>();

    public ListenerManager(RuntimeLogger logger, MllpServer.MessageHandler messageHandler) {
        this.logger = logger;
        this.messageHandler = messageHandler;
    }

    public synchronized void start(Collection<Integer> ports) {
        stop();
        TreeSet<Integer> portSet = new TreeSet<>();
        if (ports != null) {
            for (Integer port : ports) {
                if (port != null && port > 0) {
                    portSet.add(port);
                }
            }
        }
        for (Integer port : portSet) {
            MllpServer server = new MllpServer(port, logger, messageHandler);
            server.start();
            servers.put(port, server);
        }
    }

    public synchronized void stop() {
        for (MllpServer server : servers.values()) {
            try {
                server.stop();
            } catch (Exception ignored) {
            }
        }
        servers.clear();
    }

    public synchronized boolean isRunning() {
        for (MllpServer server : servers.values()) {
            if (server.isRunning()) {
                return true;
            }
        }
        return false;
    }

    public synchronized List<Integer> getActivePorts() {
        return Collections.unmodifiableList(new ArrayList<>(servers.keySet()));
    }

    public synchronized String describe() {
        if (servers.isEmpty()) {
            return "STOPPED";
        }
        return "RUNNING " + servers.keySet();
    }
}
