package com.zhunyintech.inmehl7.client.db;

import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.Hl7Observation;
import com.zhunyintech.inmehl7.client.model.Hl7ResultRecord;
import com.zhunyintech.inmehl7.client.model.OutboxRecord;
import com.zhunyintech.inmehl7.client.model.RuntimeEvent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SQLiteStore {

    private final RuntimeLogger logger;
    private final String jdbcUrl;

    public SQLiteStore(Config config, RuntimeLogger logger) {
        this.logger = logger;
        this.jdbcUrl = "jdbc:sqlite:" + resolveDbPath(config.get("dbPath"));
    }

    public void init() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("SQLite JDBC driver not found", ex);
        }
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS hl7_device_config (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "config_key TEXT UNIQUE," +
                "config_value TEXT," +
                "updated_at TEXT" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS hl7_message_raw (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "raw_message TEXT NOT NULL," +
                "valid INTEGER NOT NULL," +
                "parse_error TEXT," +
                "received_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS hl7_result_header (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "record_id TEXT NOT NULL UNIQUE," +
                "client_code TEXT NOT NULL," +
                "org_code TEXT NOT NULL," +
                "device_id TEXT," +
                "message_control_id TEXT," +
                "sample_no TEXT," +
                "patient_id TEXT," +
                "patient_name TEXT," +
                "result_time TEXT," +
                "result_type TEXT," +
                "his_target TEXT," +
                "his_request_no TEXT," +
                "raw_message TEXT," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS hl7_result_item (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "header_id INTEGER NOT NULL," +
                "item_code TEXT," +
                "item_name TEXT," +
                "value_text TEXT," +
                "unit TEXT," +
                "ref_range TEXT," +
                "abnormal_flag TEXT," +
                "observed_at TEXT," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS hl7_sync_outbox (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "header_id INTEGER NOT NULL," +
                "sync_status TEXT NOT NULL," +
                "retry_count INTEGER NOT NULL DEFAULT 0," +
                "last_error TEXT," +
                "next_retry_at TEXT," +
                "synced_at TEXT," +
                "receipt_json TEXT," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS hl7_sync_log (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "batch_id TEXT," +
                "request_json TEXT," +
                "response_json TEXT," +
                "success INTEGER," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS hl7_runtime_event (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "event_type TEXT," +
                "level TEXT," +
                "module TEXT," +
                "message TEXT," +
                "raw TEXT," +
                "event_time TEXT NOT NULL," +
                "synced INTEGER NOT NULL DEFAULT 0," +
                "synced_at TEXT" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS hl7_auth_cache (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "token TEXT," +
                "created_at TEXT NOT NULL," +
                "updated_at TEXT NOT NULL" +
                ")");
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to initialize sqlite schema", ex);
        }
    }

    public void saveRawMessage(String rawMessage, boolean valid, String parseError) {
        String sql = "INSERT INTO hl7_message_raw(raw_message, valid, parse_error, received_at) VALUES (?, ?, ?, ?)";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, rawMessage);
            ps.setInt(2, valid ? 1 : 0);
            ps.setString(3, parseError);
            ps.setString(4, formatTime(LocalDateTime.now()));
            ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("saveRawMessage failed", ex);
        }
    }

    public void saveResult(String clientCode, String orgCode, String deviceId, Hl7ResultRecord record) {
        String now = formatTime(LocalDateTime.now());
        String recordId = record.getRecordId();
        if (recordId == null || recordId.trim().isEmpty()) {
            recordId = UUID.randomUUID().toString();
            record.setRecordId(recordId);
        }

        String headerSql = "INSERT INTO hl7_result_header(" +
            "record_id, client_code, org_code, device_id, message_control_id, sample_no, patient_id, patient_name, " +
            "result_time, result_type, his_target, his_request_no, raw_message, created_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String itemSql = "INSERT INTO hl7_result_item(" +
            "header_id, item_code, item_name, value_text, unit, ref_range, abnormal_flag, observed_at, created_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String outboxSql = "INSERT INTO hl7_sync_outbox(header_id, sync_status, retry_count, created_at) VALUES (?, 'PENDING', 0, ?)";

        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            long headerId;
            try (PreparedStatement ps = conn.prepareStatement(headerSql, Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, recordId);
                ps.setString(2, clientCode);
                ps.setString(3, orgCode);
                ps.setString(4, deviceId);
                ps.setString(5, record.getMessageControlId());
                ps.setString(6, record.getSampleNo());
                ps.setString(7, record.getPatientId());
                ps.setString(8, record.getPatientName());
                ps.setString(9, formatTime(record.getResultTime()));
                ps.setString(10, record.getResultType());
                ps.setString(11, record.getHisTarget());
                ps.setString(12, record.getHisRequestNo());
                ps.setString(13, record.getRawMessage());
                ps.setString(14, now);
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (!rs.next()) {
                        throw new SQLException("No header id generated");
                    }
                    headerId = rs.getLong(1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(itemSql)) {
                for (Hl7Observation observation : record.getObservations()) {
                    ps.setLong(1, headerId);
                    ps.setString(2, observation.getItemCode());
                    ps.setString(3, observation.getItemName());
                    ps.setString(4, observation.getValue());
                    ps.setString(5, observation.getUnit());
                    ps.setString(6, observation.getReferenceRange());
                    ps.setString(7, observation.getAbnormalFlag());
                    ps.setString(8, formatTime(observation.getObservedAt()));
                    ps.setString(9, now);
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            try (PreparedStatement ps = conn.prepareStatement(outboxSql)) {
                ps.setLong(1, headerId);
                ps.setString(2, now);
                ps.executeUpdate();
            }
            conn.commit();
        } catch (SQLException ex) {
            logger.error("saveResult failed", ex);
        }
    }

    public List<OutboxRecord> fetchPendingOutbox(int limit) {
        List<OutboxRecord> records = new ArrayList<>();
        String sql = "SELECT id, header_id, retry_count FROM hl7_sync_outbox " +
            "WHERE sync_status IN ('PENDING','RETRY') " +
            "AND (next_retry_at IS NULL OR next_retry_at <= ?) ORDER BY id LIMIT ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, formatTime(LocalDateTime.now()));
            ps.setInt(2, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long outboxId = rs.getLong("id");
                    long headerId = rs.getLong("header_id");
                    int retryCount = rs.getInt("retry_count");
                    Hl7ResultRecord record = loadRecordByHeaderId(conn, headerId);
                    if (record == null) {
                        continue;
                    }
                    OutboxRecord outboxRecord = new OutboxRecord();
                    outboxRecord.setOutboxId(outboxId);
                    outboxRecord.setHeaderId(headerId);
                    outboxRecord.setRetryCount(retryCount);
                    outboxRecord.setRecord(record);
                    records.add(outboxRecord);
                }
            }
        } catch (SQLException ex) {
            logger.error("fetchPendingOutbox failed", ex);
        }
        return records;
    }

    public void markOutboxSynced(long outboxId, String receiptJson) {
        String sql = "UPDATE hl7_sync_outbox SET sync_status='SYNCED', synced_at=?, receipt_json=?, last_error=NULL WHERE id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, formatTime(LocalDateTime.now()));
            ps.setString(2, receiptJson);
            ps.setLong(3, outboxId);
            ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("markOutboxSynced failed", ex);
        }
    }

    public void markOutboxRetry(long outboxId, String error, int retryIntervalSec) {
        String sql = "UPDATE hl7_sync_outbox SET sync_status='RETRY', retry_count=retry_count+1, last_error=?, next_retry_at=? WHERE id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, error);
            ps.setString(2, formatTime(LocalDateTime.now().plusSeconds(Math.max(5, retryIntervalSec))));
            ps.setLong(3, outboxId);
            ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("markOutboxRetry failed", ex);
        }
    }

    public void saveSyncLog(String batchId, String requestJson, String responseJson, boolean success) {
        String sql = "INSERT INTO hl7_sync_log(batch_id, request_json, response_json, success, created_at) VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, batchId);
            ps.setString(2, requestJson);
            ps.setString(3, responseJson);
            ps.setInt(4, success ? 1 : 0);
            ps.setString(5, formatTime(LocalDateTime.now()));
            ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("saveSyncLog failed", ex);
        }
    }

    public void saveRuntimeEvent(String type, String level, String module, String message, String raw) {
        String sql = "INSERT INTO hl7_runtime_event(event_type, level, module, message, raw, event_time, synced) VALUES (?, ?, ?, ?, ?, ?, 0)";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, type);
            ps.setString(2, level);
            ps.setString(3, module);
            ps.setString(4, message);
            ps.setString(5, raw);
            ps.setString(6, formatTime(LocalDateTime.now()));
            ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("saveRuntimeEvent failed", ex);
        }
    }

    public List<RuntimeEvent> fetchPendingRuntimeEvents(int limit) {
        List<RuntimeEvent> events = new ArrayList<>();
        String sql = "SELECT id, event_type, level, module, message, raw, event_time FROM hl7_runtime_event WHERE synced=0 ORDER BY id LIMIT ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    RuntimeEvent event = new RuntimeEvent();
                    event.setId(rs.getLong("id"));
                    event.setType(rs.getString("event_type"));
                    event.setLevel(rs.getString("level"));
                    event.setModule(rs.getString("module"));
                    event.setMessage(rs.getString("message"));
                    event.setRaw(rs.getString("raw"));
                    event.setEventTime(parseTime(rs.getString("event_time")));
                    events.add(event);
                }
            }
        } catch (SQLException ex) {
            logger.error("fetchPendingRuntimeEvents failed", ex);
        }
        return events;
    }

    public void markRuntimeEventsSynced(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        StringBuilder sql = new StringBuilder("UPDATE hl7_runtime_event SET synced=1, synced_at=? WHERE id IN (");
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sql.append(",");
            }
            sql.append("?");
        }
        sql.append(")");

        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            ps.setString(1, formatTime(LocalDateTime.now()));
            for (int i = 0; i < ids.size(); i++) {
                ps.setLong(i + 2, ids.get(i));
            }
            ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("markRuntimeEventsSynced failed", ex);
        }
    }

    public void saveAuthToken(String token) {
        String query = "SELECT id FROM hl7_auth_cache ORDER BY id DESC LIMIT 1";
        String insert = "INSERT INTO hl7_auth_cache(token, created_at, updated_at) VALUES (?, ?, ?)";
        String update = "UPDATE hl7_auth_cache SET token=?, updated_at=? WHERE id=?";
        String now = formatTime(LocalDateTime.now());
        try (Connection conn = connect();
             PreparedStatement queryPs = conn.prepareStatement(query);
             ResultSet rs = queryPs.executeQuery()) {
            if (rs.next()) {
                long id = rs.getLong("id");
                try (PreparedStatement ps = conn.prepareStatement(update)) {
                    ps.setString(1, token);
                    ps.setString(2, now);
                    ps.setLong(3, id);
                    ps.executeUpdate();
                }
            } else {
                try (PreparedStatement ps = conn.prepareStatement(insert)) {
                    ps.setString(1, token);
                    ps.setString(2, now);
                    ps.setString(3, now);
                    ps.executeUpdate();
                }
            }
        } catch (SQLException ex) {
            logger.error("saveAuthToken failed", ex);
        }
    }

    public String loadAuthToken() {
        String sql = "SELECT token FROM hl7_auth_cache ORDER BY id DESC LIMIT 1";
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("token");
            }
        } catch (SQLException ex) {
            logger.error("loadAuthToken failed", ex);
        }
        return null;
    }

    private Hl7ResultRecord loadRecordByHeaderId(Connection conn, long headerId) throws SQLException {
        String headerSql = "SELECT record_id, message_control_id, sample_no, patient_id, patient_name, result_time, result_type, his_target, his_request_no, raw_message " +
            "FROM hl7_result_header WHERE id=?";
        Hl7ResultRecord record = null;
        try (PreparedStatement ps = conn.prepareStatement(headerSql)) {
            ps.setLong(1, headerId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    record = new Hl7ResultRecord();
                    record.setRecordId(rs.getString("record_id"));
                    record.setMessageControlId(rs.getString("message_control_id"));
                    record.setSampleNo(rs.getString("sample_no"));
                    record.setPatientId(rs.getString("patient_id"));
                    record.setPatientName(rs.getString("patient_name"));
                    record.setResultTime(parseTime(rs.getString("result_time")));
                    record.setResultType(rs.getString("result_type"));
                    record.setHisTarget(rs.getString("his_target"));
                    record.setHisRequestNo(rs.getString("his_request_no"));
                    record.setRawMessage(rs.getString("raw_message"));
                }
            }
        }
        if (record == null) {
            return null;
        }
        String itemSql = "SELECT item_code, item_name, value_text, unit, ref_range, abnormal_flag, observed_at FROM hl7_result_item WHERE header_id=? ORDER BY id";
        try (PreparedStatement ps = conn.prepareStatement(itemSql)) {
            ps.setLong(1, headerId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Hl7Observation item = new Hl7Observation();
                    item.setItemCode(rs.getString("item_code"));
                    item.setItemName(rs.getString("item_name"));
                    item.setValue(rs.getString("value_text"));
                    item.setUnit(rs.getString("unit"));
                    item.setReferenceRange(rs.getString("ref_range"));
                    item.setAbnormalFlag(rs.getString("abnormal_flag"));
                    item.setObservedAt(parseTime(rs.getString("observed_at")));
                    record.getObservations().add(item);
                }
            }
        }
        return record;
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    private String resolveDbPath(String configuredPath) {
        String pathValue = (configuredPath == null || configuredPath.trim().isEmpty())
            ? "./data/inmehl7-client.db"
            : configuredPath.trim();
        Path dbPath = Paths.get(pathValue).toAbsolutePath().normalize();
        Path parent = dbPath.getParent();
        try {
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }
        } catch (Exception ignored) {
        }
        return dbPath.toString();
    }

    private String formatTime(LocalDateTime value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    private LocalDateTime parseTime(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return LocalDateTime.parse(value.trim());
    }
}

