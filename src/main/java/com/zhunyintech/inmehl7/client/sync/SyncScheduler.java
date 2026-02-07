package com.zhunyintech.inmehl7.client.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.db.SQLiteStore;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.Hl7Observation;
import com.zhunyintech.inmehl7.client.model.Hl7ResultRecord;
import com.zhunyintech.inmehl7.client.model.LicenseSnapshot;
import com.zhunyintech.inmehl7.client.model.OutboxRecord;
import com.zhunyintech.inmehl7.client.model.RuntimeEvent;
import com.zhunyintech.inmehl7.client.state.AppState;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SyncScheduler {

    private final Config config;
    private final RuntimeLogger logger;
    private final SQLiteStore store;
    private final AppState appState;
    private final ServerApiClient apiClient;
    private final Supplier<Boolean> uploaderRunningSupplier;
    private final Consumer<String> uiStatusConsumer;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final AtomicBoolean started = new AtomicBoolean(false);

    private ScheduledExecutorService scheduler;

    public SyncScheduler(Config config,
                         RuntimeLogger logger,
                         SQLiteStore store,
                         AppState appState,
                         ServerApiClient apiClient,
                         Supplier<Boolean> uploaderRunningSupplier,
                         Consumer<String> uiStatusConsumer) {
        this.config = config;
        this.logger = logger;
        this.store = store;
        this.appState = appState;
        this.apiClient = apiClient;
        this.uploaderRunningSupplier = uploaderRunningSupplier;
        this.uiStatusConsumer = uiStatusConsumer;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        scheduler = Executors.newScheduledThreadPool(3);
        int heartbeatSec = config.getInt("heartbeatIntervalSec", 60);
        int syncSec = config.getInt("syncIntervalSec", 60);
        scheduler.scheduleWithFixedDelay(this::heartbeatSafe, 3, Math.max(10, heartbeatSec), TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::syncResultsSafe, 5, Math.max(10, syncSec), TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::syncLogsSafe, 8, Math.max(15, syncSec), TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::refreshAuthSafe, 2, 300, TimeUnit.SECONDS);
        logger.info("Sync scheduler started");
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        logger.info("Sync scheduler stopped");
    }

    public void triggerSyncNow() {
        if (!started.get()) {
            return;
        }
        scheduler.submit(this::syncResultsSafe);
        scheduler.submit(this::syncLogsSafe);
        scheduler.submit(this::heartbeatSafe);
    }

    private void refreshAuthSafe() {
        try {
            refreshAuth();
        } catch (Exception ex) {
            logger.warn("refreshAuth failed: " + ex.getMessage());
        }
    }

    private void heartbeatSafe() {
        try {
            heartbeat();
        } catch (Exception ex) {
            logger.warn("heartbeat failed: " + ex.getMessage());
        }
    }

    private void syncResultsSafe() {
        try {
            syncResults();
        } catch (Exception ex) {
            logger.warn("syncResults failed: " + ex.getMessage());
        }
    }

    private void syncLogsSafe() {
        try {
            syncLogs();
        } catch (Exception ex) {
            logger.warn("syncLogs failed: " + ex.getMessage());
        }
    }

    private void refreshAuth() throws IOException, InterruptedException {
        String token = appState.getToken();
        if (token == null || token.trim().isEmpty()) {
            return;
        }
        Map<String, Object> req = new HashMap<>();
        req.put("clientCode", config.get("clientCode"));
        req.put("clientName", config.get("clientName"));
        req.put("orgCode", config.get("orgCode"));
        req.put("deviceId", config.get("deviceId"));
        req.put("deviceFingerprint", config.get("deviceFingerprint"));
        req.put("appVersion", config.get("appVersion"));
        JsonNode data = apiClient.refreshAuth(token, req);
        applyLicenseSnapshot(data);
    }

    private void heartbeat() throws IOException, InterruptedException {
        String token = appState.getToken();
        if (token == null || token.trim().isEmpty()) {
            return;
        }
        Map<String, Object> req = new HashMap<>();
        req.put("clientCode", config.get("clientCode"));
        req.put("orgCode", config.get("orgCode"));
        req.put("account", null);
        req.put("clientIp", localIp());
        req.put("macAddr", localMac());
        req.put("appVersion", config.get("appVersion"));
        req.put("uploaderStatus", uploaderRunningSupplier.get() ? "RUNNING" : "STOPPED");
        req.put("extraJson", "{\"time\":\"" + LocalDateTime.now() + "\"}");
        JsonNode data = apiClient.heartbeat(token, req);
        applyLicenseSnapshot(data);
    }

    private void syncResults() throws IOException, InterruptedException {
        String token = appState.getToken();
        if (token == null || token.trim().isEmpty()) {
            return;
        }
        List<OutboxRecord> outboxes = store.fetchPendingOutbox(config.getInt("maxSyncBatch", 50));
        if (outboxes.isEmpty()) {
            return;
        }
        String batchId = UUID.randomUUID().toString();

        Map<String, Object> req = new HashMap<>();
        req.put("batchId", batchId);
        req.put("clientCode", config.get("clientCode"));
        req.put("orgCode", config.get("orgCode"));
        req.put("deviceId", config.get("deviceId"));
        req.put("appVersion", config.get("appVersion"));
        List<Map<String, Object>> items = new ArrayList<>();
        for (OutboxRecord outbox : outboxes) {
            Hl7ResultRecord record = outbox.getRecord();
            Map<String, Object> row = new HashMap<>();
            row.put("clientRecordId", record.getRecordId());
            row.put("messageControlId", record.getMessageControlId());
            row.put("sampleNo", record.getSampleNo());
            row.put("patientId", record.getPatientId());
            row.put("patientName", record.getPatientName());
            row.put("resultTime", record.getResultTime());
            row.put("resultType", record.getResultType());
            row.put("hisTarget", record.getHisTarget());
            row.put("hisRequestNo", record.getHisRequestNo());
            row.put("rawMessage", record.getRawMessage());

            List<Map<String, Object>> observations = new ArrayList<>();
            for (Hl7Observation obs : record.getObservations()) {
                Map<String, Object> obsRow = new HashMap<>();
                obsRow.put("itemCode", obs.getItemCode());
                obsRow.put("itemName", obs.getItemName());
                obsRow.put("value", obs.getValue());
                obsRow.put("unit", obs.getUnit());
                obsRow.put("referenceRange", obs.getReferenceRange());
                obsRow.put("abnormalFlag", obs.getAbnormalFlag());
                obsRow.put("observedAt", obs.getObservedAt());
                observations.add(obsRow);
            }
            row.put("observations", observations);
            items.add(row);
        }
        req.put("items", items);

        String reqJson = mapper.writeValueAsString(req);
        JsonNode data = apiClient.syncResults(token, req);
        String respJson = data.toString();

        Map<String, String> statusByRecordId = new HashMap<>();
        Map<String, String> messageByRecordId = new HashMap<>();
        JsonNode itemNode = data.get("items");
        if (itemNode != null && itemNode.isArray()) {
            for (JsonNode node : itemNode) {
                String recordId = text(node, "clientRecordId");
                statusByRecordId.put(recordId, text(node, "status"));
                messageByRecordId.put(recordId, text(node, "message"));
            }
        }

        int retrySec = config.getInt("retryIntervalSec", 30);
        for (OutboxRecord outbox : outboxes) {
            String recordId = outbox.getRecord().getRecordId();
            String status = statusByRecordId.get(recordId);
            if ("ACCEPTED".equalsIgnoreCase(status) || "DUPLICATED".equalsIgnoreCase(status)) {
                store.markOutboxSynced(outbox.getOutboxId(), respJson);
            } else {
                store.markOutboxRetry(outbox.getOutboxId(), messageByRecordId.get(recordId), retrySec);
            }
        }
        store.saveSyncLog(batchId, reqJson, respJson, true);
        logger.info("Result batch synced, batchId=" + batchId + ", count=" + outboxes.size());
    }

    private void syncLogs() throws IOException, InterruptedException {
        String token = appState.getToken();
        if (token == null || token.trim().isEmpty()) {
            return;
        }
        List<RuntimeEvent> events = store.fetchPendingRuntimeEvents(200);
        if (events.isEmpty()) {
            return;
        }
        Map<String, Object> req = new HashMap<>();
        req.put("clientCode", config.get("clientCode"));
        req.put("orgCode", config.get("orgCode"));
        req.put("fileName", "runtime-event");
        req.put("appVersion", config.get("appVersion"));
        List<Map<String, Object>> entries = new ArrayList<>();
        for (RuntimeEvent event : events) {
            Map<String, Object> row = new HashMap<>();
            row.put("type", event.getType());
            row.put("level", event.getLevel());
            row.put("module", event.getModule());
            row.put("logTime", event.getEventTime());
            row.put("message", event.getMessage());
            row.put("raw", event.getRaw());
            entries.add(row);
        }
        req.put("entries", entries);

        apiClient.syncLogs(token, req);
        List<Long> ids = new ArrayList<>();
        for (RuntimeEvent event : events) {
            ids.add(event.getId());
        }
        store.markRuntimeEventsSynced(ids);
    }

    private void applyLicenseSnapshot(JsonNode data) {
        if (data == null || data.isNull()) {
            return;
        }
        LicenseSnapshot snapshot = new LicenseSnapshot();
        snapshot.setLicenseStatus(text(data, "licenseStatus"));
        snapshot.setDaysLeft(number(data, "daysLeft"));
        snapshot.setMessage(text(data, "message"));
        appState.setLicenseSnapshot(snapshot);

        String line = "License status: " + snapshot.getLicenseStatus()
            + ", daysLeft=" + snapshot.getDaysLeft()
            + ", message=" + snapshot.getMessage();
        logger.info(line);
        if (uiStatusConsumer != null) {
            uiStatusConsumer.accept(line);
        }

        long daysLeft = snapshot.getDaysLeft();
        if (daysLeft <= 30) {
            store.saveRuntimeEvent("LICENSE", "WARN", "auth", line, null);
        }
    }

    private String text(JsonNode node, String field) {
        if (node == null || node.get(field) == null || node.get(field).isNull()) {
            return null;
        }
        return node.get(field).asText();
    }

    private long number(JsonNode node, String field) {
        if (node == null || node.get(field) == null || node.get(field).isNull()) {
            return -1;
        }
        return node.get(field).asLong(-1);
    }

    private String localIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception ignored) {
            return "127.0.0.1";
        }
    }

    private String localMac() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface net = interfaces.nextElement();
                if (net.isLoopback() || !net.isUp()) {
                    continue;
                }
                byte[] mac = net.getHardwareAddress();
                if (mac == null || mac.length == 0) {
                    continue;
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < mac.length; i++) {
                    sb.append(String.format("%02X", mac[i]));
                    if (i < mac.length - 1) {
                        sb.append("-");
                    }
                }
                return sb.toString();
            }
        } catch (Exception ignored) {
        }
        return "UNKNOWN";
    }
}

