package com.zhunyintech.inmehl7.client.db;

import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.hl7.MllpServer;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.DeviceRegistry;
import com.zhunyintech.inmehl7.client.model.Hl7Observation;
import com.zhunyintech.inmehl7.client.model.Hl7ResultRecord;
import com.zhunyintech.inmehl7.client.model.MedicalDataItem;
import com.zhunyintech.inmehl7.client.model.MedicalDataRecord;
import com.zhunyintech.inmehl7.client.model.OrgInfo;
import com.zhunyintech.inmehl7.client.ventilator.VentilatorRecordSupport;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public class ClientRepository {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Config config;
    private final RuntimeLogger logger;
    private final String jdbcUrl;

    public ClientRepository(Config config, RuntimeLogger logger) {
        this.config = config;
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
            st.execute("CREATE TABLE IF NOT EXISTS client_org_info (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "org_code TEXT NOT NULL UNIQUE," +
                "org_name TEXT," +
                "his_code TEXT," +
                "si_org_code TEXT," +
                "webservice_url TEXT," +
                "sso_account TEXT," +
                "oper_id TEXT," +
                "real_name TEXT," +
                "token_snapshot TEXT," +
                "last_login_at TEXT," +
                "ext_json TEXT," +
                "created_at TEXT NOT NULL," +
                "updated_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS client_device_registry (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "org_code TEXT NOT NULL," +
                "device_type TEXT NOT NULL," +
                "protocol_code TEXT NOT NULL," +
                "vendor_name TEXT," +
                "model_name TEXT," +
                "device_id TEXT NOT NULL," +
                "mac_address TEXT," +
                "source_ip TEXT," +
                "source_port INTEGER," +
                "listen_port INTEGER," +
                "nickname TEXT," +
                "ward_code TEXT," +
                "ward_name TEXT," +
                "bed_code TEXT," +
                "bed_name TEXT," +
                "patient_id TEXT," +
                "patient_name TEXT," +
                "patient_no TEXT," +
                "admission_no TEXT," +
                "bind_source TEXT," +
                "ack_mode TEXT," +
                "enabled INTEGER NOT NULL DEFAULT 1," +
                "last_seen_at TEXT," +
                "ext_json TEXT," +
                "created_at TEXT NOT NULL," +
                "updated_at TEXT NOT NULL," +
                "UNIQUE(org_code, device_id)" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS client_device_patient_binding (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "org_code TEXT NOT NULL," +
                "device_id TEXT NOT NULL," +
                "patient_id TEXT," +
                "patient_name TEXT," +
                "patient_no TEXT," +
                "admission_no TEXT," +
                "ward_name TEXT," +
                "bed_name TEXT," +
                "bind_source TEXT," +
                "effective_at TEXT NOT NULL," +
                "expired_at TEXT," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS client_medical_data_raw (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "org_code TEXT NOT NULL," +
                "device_id TEXT," +
                "device_type TEXT," +
                "source_ip TEXT," +
                "source_port INTEGER," +
                "listen_port INTEGER," +
                "protocol_code TEXT NOT NULL," +
                "hl7_version TEXT," +
                "message_type TEXT," +
                "trigger_event TEXT," +
                "message_control_id TEXT," +
                "charset TEXT," +
                "ack_code TEXT," +
                "raw_payload TEXT NOT NULL," +
                "raw_hash TEXT NOT NULL UNIQUE," +
                "receive_time TEXT NOT NULL," +
                "parse_status TEXT NOT NULL," +
                "parse_error TEXT," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS client_medical_data_header (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "raw_id INTEGER," +
                "org_code TEXT NOT NULL," +
                "device_id TEXT NOT NULL," +
                "device_type TEXT NOT NULL," +
                "device_name TEXT," +
                "data_category TEXT NOT NULL," +
                "protocol_code TEXT NOT NULL," +
                "message_type TEXT," +
                "trigger_event TEXT," +
                "message_control_id TEXT," +
                "patient_id TEXT," +
                "patient_name TEXT," +
                "patient_gender TEXT," +
                "patient_age TEXT," +
                "ward_name TEXT," +
                "bed_name TEXT," +
                "visit_id TEXT," +
                "sample_no TEXT," +
                "report_no TEXT," +
                "observation_time TEXT," +
                "report_time TEXT," +
                "receive_time TEXT," +
                "record_status TEXT NOT NULL," +
                "data_hash TEXT NOT NULL UNIQUE," +
                "summary_json TEXT," +
                "created_at TEXT NOT NULL," +
                "updated_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS client_medical_data_item (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "header_id INTEGER NOT NULL," +
                "segment_type TEXT," +
                "item_code TEXT," +
                "item_name TEXT," +
                "value_type TEXT," +
                "value_text TEXT," +
                "numeric_value REAL," +
                "unit TEXT," +
                "ref_range TEXT," +
                "abnormal_flag TEXT," +
                "observed_at TEXT," +
                "sort_no INTEGER," +
                "ext_json TEXT," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS client_medical_data_attachment (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "header_id INTEGER NOT NULL," +
                "file_name TEXT NOT NULL," +
                "file_path TEXT NOT NULL," +
                "file_type TEXT," +
                "mime_type TEXT," +
                "file_size INTEGER," +
                "checksum TEXT," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS client_export_task (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "org_code TEXT NOT NULL," +
                "device_id TEXT," +
                "export_type TEXT NOT NULL," +
                "range_from TEXT," +
                "range_to TEXT," +
                "record_count INTEGER," +
                "file_path TEXT," +
                "operator_name TEXT," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE TABLE IF NOT EXISTS client_runtime_event (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "event_type TEXT," +
                "level TEXT," +
                "module TEXT," +
                "message TEXT," +
                "raw TEXT," +
                "event_time TEXT," +
                "created_at TEXT NOT NULL" +
                ")");
            st.execute("CREATE INDEX IF NOT EXISTS idx_client_device_route ON client_device_registry(org_code, source_ip, listen_port)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_client_device_type ON client_device_registry(org_code, device_type)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_client_raw_device_time ON client_medical_data_raw(device_id, receive_time DESC)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_client_header_device_time ON client_medical_data_header(device_id, report_time DESC)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_client_header_patient_time ON client_medical_data_header(patient_id, report_time DESC)");
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to initialize client sqlite schema", ex);
        }
    }

    public OrgInfo loadOrgInfo() {
        String preferredOrgCode = blankToNull(config.get("orgCode"));
        String sql = preferredOrgCode == null
            ? "SELECT * FROM client_org_info ORDER BY updated_at DESC, id DESC LIMIT 1"
            : "SELECT * FROM client_org_info WHERE org_code=? ORDER BY updated_at DESC, id DESC LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            if (preferredOrgCode != null) {
                ps.setString(1, preferredOrgCode);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapOrgInfo(rs);
                }
            }
        } catch (SQLException ex) {
            logger.error("loadOrgInfo failed", ex);
        }
        return null;
    }

    public synchronized void saveOrgInfo(OrgInfo orgInfo) {
        if (orgInfo == null || isBlank(orgInfo.getOrgCode())) {
            return;
        }
        String now = formatTime(LocalDateTime.now());
        String sql = "INSERT INTO client_org_info(" +
            "org_code, org_name, his_code, si_org_code, webservice_url, sso_account, oper_id, real_name, token_snapshot, last_login_at, created_at, updated_at" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(org_code) DO UPDATE SET " +
            "org_name=excluded.org_name, his_code=excluded.his_code, si_org_code=excluded.si_org_code, " +
            "webservice_url=excluded.webservice_url, sso_account=excluded.sso_account, oper_id=excluded.oper_id, " +
            "real_name=excluded.real_name, token_snapshot=excluded.token_snapshot, last_login_at=excluded.last_login_at, updated_at=excluded.updated_at";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, orgInfo.getOrgCode().trim());
            ps.setString(2, blankToNull(orgInfo.getOrgName()));
            ps.setString(3, blankToNull(orgInfo.getHisCode()));
            ps.setString(4, blankToNull(orgInfo.getSiOrgCode()));
            ps.setString(5, blankToNull(orgInfo.getWebserviceUrl()));
            ps.setString(6, blankToNull(orgInfo.getSsoAccount()));
            ps.setString(7, blankToNull(orgInfo.getOperId()));
            ps.setString(8, blankToNull(orgInfo.getRealName()));
            ps.setString(9, blankToNull(orgInfo.getTokenSnapshot()));
            ps.setString(10, formatTime(orgInfo.getLastLoginAt()));
            ps.setString(11, now);
            ps.setString(12, now);
            ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("saveOrgInfo failed", ex);
        }
    }

    public synchronized void clearOrgInfo() {
        String sql = "DELETE FROM client_org_info";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("clearOrgInfo failed", ex);
        }
    }

    public List<DeviceRegistry> listDevices(String orgCode) {
        List<DeviceRegistry> devices = new ArrayList<>();
        String sql = isBlank(orgCode)
            ? "SELECT * FROM client_device_registry ORDER BY enabled DESC, updated_at DESC, id DESC"
            : "SELECT * FROM client_device_registry WHERE org_code=? ORDER BY enabled DESC, updated_at DESC, id DESC";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            if (!isBlank(orgCode)) {
                ps.setString(1, orgCode.trim());
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    devices.add(mapDevice(rs));
                }
            }
        } catch (SQLException ex) {
            logger.error("listDevices failed", ex);
        }
        return devices;
    }

    public synchronized DeviceRegistry saveDevice(DeviceRegistry device) {
        if (device == null) {
            return null;
        }
        device.setOrgCode(defaultOrgCode(device.getOrgCode()));
        if (isBlank(device.getDeviceId())) {
            device.setDeviceId(buildAutoDeviceId(device.getSourceIp(), device.getListenPort()));
        }
        String now = formatTime(LocalDateTime.now());
        String sql = "INSERT INTO client_device_registry(" +
            "org_code, device_type, protocol_code, vendor_name, model_name, device_id, mac_address, source_ip, source_port, listen_port, nickname, ward_code, ward_name, bed_code, bed_name, " +
            "patient_id, patient_name, patient_no, admission_no, bind_source, ack_mode, enabled, last_seen_at, ext_json, created_at, updated_at" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(org_code, device_id) DO UPDATE SET " +
            "device_type=excluded.device_type, protocol_code=excluded.protocol_code, vendor_name=excluded.vendor_name, model_name=excluded.model_name, mac_address=excluded.mac_address, " +
            "source_ip=excluded.source_ip, source_port=excluded.source_port, listen_port=excluded.listen_port, nickname=excluded.nickname, ward_code=excluded.ward_code, ward_name=excluded.ward_name, " +
            "bed_code=excluded.bed_code, bed_name=excluded.bed_name, patient_id=excluded.patient_id, patient_name=excluded.patient_name, patient_no=excluded.patient_no, admission_no=excluded.admission_no, " +
            "bind_source=excluded.bind_source, ack_mode=excluded.ack_mode, enabled=excluded.enabled, last_seen_at=excluded.last_seen_at, ext_json=excluded.ext_json, updated_at=excluded.updated_at";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            fillDeviceStatement(ps, device, now);
            ps.executeUpdate();
            updateBindingHistory(conn, device, now);
            DeviceRegistry latest = selectDeviceByOrgAndId(conn, device.getOrgCode(), device.getDeviceId());
            if (latest != null) {
                device.setId(latest.getId());
                return latest;
            }
        } catch (SQLException ex) {
            logger.error("saveDevice failed", ex);
        }
        return device;
    }

    public synchronized DeviceRegistry resolveOrCreateDevice(String orgCode,
                                                             int listenPort,
                                                             String sourceIp,
                                                             int sourcePort,
                                                             Hl7ResultRecord record) {
        String resolvedOrgCode = defaultOrgCode(orgCode);
        try (Connection conn = connect()) {
            DeviceRegistry device = selectDeviceByRoute(conn, resolvedOrgCode, listenPort, sourceIp);
            if (device == null && record != null && !isBlank(record.getSourceApplication())) {
                device = selectDeviceByOrgAndId(conn, resolvedOrgCode, record.getSourceApplication());
            }
            if (device == null) {
                DeviceRegistry autoDevice = buildAutoDevice(resolvedOrgCode, listenPort, sourceIp, sourcePort, record);
                return saveDevice(autoDevice);
            }
            device.setSourceIp(firstNonBlank(sourceIp, device.getSourceIp()));
            if (sourcePort > 0) {
                device.setSourcePort(sourcePort);
            }
            device.setListenPort(listenPort > 0 ? listenPort : device.getListenPort());
            device.setLastSeenAt(LocalDateTime.now());
            if (record != null) {
                if (isBlank(device.getWardName())) {
                    device.setWardName(record.getWardName());
                }
                if (isBlank(device.getBedName())) {
                    device.setBedName(record.getBedName());
                }
                if (isBlank(device.getPatientId())) {
                    device.setPatientId(record.getPatientId());
                }
                if (isBlank(device.getPatientName())) {
                    device.setPatientName(record.getPatientName());
                }
                if ("UNKNOWN".equalsIgnoreCase(blankToNull(device.getDeviceType()))) {
                    device.setDeviceType(inferDeviceType(record));
                }
            }
            return saveDevice(device);
        } catch (SQLException ex) {
            logger.error("resolveOrCreateDevice failed", ex);
            return buildAutoDevice(resolvedOrgCode, listenPort, sourceIp, sourcePort, record);
        }
    }

    public int countEnabledDevices(String orgCode) {
        String sql = isBlank(orgCode)
            ? "SELECT COUNT(1) FROM client_device_registry WHERE enabled=1"
            : "SELECT COUNT(1) FROM client_device_registry WHERE org_code=? AND enabled=1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            if (!isBlank(orgCode)) {
                ps.setString(1, orgCode.trim());
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException ex) {
            logger.error("countEnabledDevices failed", ex);
            return 0;
        }
    }

    public List<Integer> listActiveListenPorts(String orgCode, int fallbackPort) {
        LinkedHashSet<Integer> ports = new LinkedHashSet<>();
        String sql = isBlank(orgCode)
            ? "SELECT DISTINCT listen_port FROM client_device_registry WHERE enabled=1 AND listen_port IS NOT NULL AND listen_port > 0 ORDER BY listen_port"
            : "SELECT DISTINCT listen_port FROM client_device_registry WHERE org_code=? AND enabled=1 AND listen_port IS NOT NULL AND listen_port > 0 ORDER BY listen_port";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            if (!isBlank(orgCode)) {
                ps.setString(1, orgCode.trim());
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int port = rs.getInt(1);
                    if (port > 0) {
                        ports.add(port);
                    }
                }
            }
        } catch (SQLException ex) {
            logger.error("listActiveListenPorts failed", ex);
        }
        if (ports.isEmpty()) {
            ports.add(fallbackPort > 0 ? fallbackPort : 5555);
        }
        return new ArrayList<>(ports);
    }

    private void fillDeviceStatement(PreparedStatement ps, DeviceRegistry device, String now) throws SQLException {
        ps.setString(1, device.getOrgCode());
        ps.setString(2, firstNonBlank(device.getDeviceType(), "UNKNOWN"));
        ps.setString(3, firstNonBlank(device.getProtocolCode(), "HL7V2_MLLP"));
        ps.setString(4, blankToNull(device.getVendorName()));
        ps.setString(5, blankToNull(device.getModelName()));
        ps.setString(6, device.getDeviceId().trim());
        ps.setString(7, blankToNull(device.getMacAddress()));
        ps.setString(8, blankToNull(device.getSourceIp()));
        if (device.getSourcePort() > 0) {
            ps.setInt(9, device.getSourcePort());
        } else {
            ps.setObject(9, null);
        }
        if (device.getListenPort() > 0) {
            ps.setInt(10, device.getListenPort());
        } else {
            ps.setObject(10, null);
        }
        ps.setString(11, blankToNull(device.getNickname()));
        ps.setString(12, blankToNull(device.getWardCode()));
        ps.setString(13, blankToNull(device.getWardName()));
        ps.setString(14, blankToNull(device.getBedCode()));
        ps.setString(15, blankToNull(device.getBedName()));
        ps.setString(16, blankToNull(device.getPatientId()));
        ps.setString(17, blankToNull(device.getPatientName()));
        ps.setString(18, blankToNull(device.getPatientNo()));
        ps.setString(19, blankToNull(device.getAdmissionNo()));
        ps.setString(20, firstNonBlank(device.getBindSource(), "LOCAL"));
        ps.setString(21, firstNonBlank(device.getAckMode(), "OPTIONAL"));
        ps.setInt(22, device.isEnabled() ? 1 : 0);
        ps.setString(23, formatTime(device.getLastSeenAt()));
        ps.setString(24, blankToNull(device.getExtJson()));
        ps.setString(25, now);
        ps.setString(26, now);
    }

    public synchronized void saveInboundRecord(String orgCode,
                                               DeviceRegistry device,
                                               MllpServer.MessageContext context,
                                               Hl7ResultRecord record,
                                               boolean accepted,
                                               String parseError) {
        String resolvedOrgCode = defaultOrgCode(orgCode);
        String rawPayload = context == null ? null : context.getHl7Message();
        if (isBlank(rawPayload)) {
            return;
        }
        String receiveTime = formatTime(LocalDateTime.now());
        String rawHash = sha256(rawPayload);
        String ackCode = accepted ? "AA" : "AR";
        String parseStatus = record == null
            ? "FAILED"
            : (device == null ? "PENDING_BIND" : "SUCCESS");
        String deviceId = device == null ? null : device.getDeviceId();
        String deviceType = device == null ? inferDeviceType(record) : device.getDeviceType();

        String rawInsert = "INSERT INTO client_medical_data_raw(" +
            "org_code, device_id, device_type, source_ip, source_port, listen_port, protocol_code, hl7_version, message_type, trigger_event, message_control_id, charset, ack_code, raw_payload, raw_hash, receive_time, parse_status, parse_error, created_at" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = connect()) {
            long rawId = findRawIdByHash(conn, rawHash);
            if (rawId <= 0) {
                try (PreparedStatement ps = conn.prepareStatement(rawInsert, Statement.RETURN_GENERATED_KEYS)) {
                    ps.setString(1, resolvedOrgCode);
                    ps.setString(2, blankToNull(deviceId));
                    ps.setString(3, blankToNull(deviceType));
                    ps.setString(4, context == null ? null : blankToNull(context.getRemoteIp()));
                    if (context != null && context.getRemotePort() > 0) {
                        ps.setInt(5, context.getRemotePort());
                    } else {
                        ps.setObject(5, null);
                    }
                    if (context != null && context.getListenPort() > 0) {
                        ps.setInt(6, context.getListenPort());
                    } else {
                        ps.setObject(6, null);
                    }
                    ps.setString(7, "HL7V2_MLLP");
                    ps.setString(8, record == null ? null : blankToNull(record.getHl7Version()));
                    ps.setString(9, record == null ? null : blankToNull(record.getMessageType()));
                    ps.setString(10, record == null ? null : blankToNull(record.getTriggerEvent()));
                    ps.setString(11, record == null ? null : blankToNull(record.getMessageControlId()));
                    ps.setString(12, StandardCharsets.UTF_8.name());
                    ps.setString(13, ackCode);
                    ps.setString(14, rawPayload);
                    ps.setString(15, rawHash);
                    ps.setString(16, receiveTime);
                    ps.setString(17, parseStatus);
                    ps.setString(18, blankToNull(parseError));
                    ps.setString(19, receiveTime);
                    ps.executeUpdate();
                    try (ResultSet rs = ps.getGeneratedKeys()) {
                        if (rs.next()) {
                            rawId = rs.getLong(1);
                        }
                    }
                }
            }
            if (record != null && rawId > 0) {
                saveHeaderAndItems(conn, resolvedOrgCode, device, record, rawId, rawHash, receiveTime, parseStatus);
            }
        } catch (SQLException ex) {
            logger.error("saveInboundRecord failed", ex);
        }
    }

    public List<MedicalDataRecord> queryRecords(String orgCode, String deviceId, String patientKeyword, int limit) {
        List<MedicalDataRecord> records = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT h.*, r.raw_payload, r.parse_status, r.parse_error, COALESCE(h.device_name, d.nickname, d.device_id, h.device_id) AS display_name ")
            .append("FROM client_medical_data_header h ")
            .append("LEFT JOIN client_medical_data_raw r ON r.id=h.raw_id ")
            .append("LEFT JOIN client_device_registry d ON d.org_code=h.org_code AND d.device_id=h.device_id ")
            .append("WHERE 1=1 ");
        List<String> params = new ArrayList<>();
        if (!isBlank(orgCode)) {
            sql.append("AND h.org_code=? ");
            params.add(orgCode.trim());
        }
        if (!isBlank(deviceId)) {
            sql.append("AND h.device_id=? ");
            params.add(deviceId.trim());
        }
        if (!isBlank(patientKeyword)) {
            sql.append("AND (IFNULL(h.patient_name,'') LIKE ? OR IFNULL(h.patient_id,'') LIKE ?) ");
            String keyword = "%" + patientKeyword.trim() + "%";
            params.add(keyword);
            params.add(keyword);
        }
        sql.append("ORDER BY IFNULL(h.receive_time, h.report_time) DESC, h.id DESC LIMIT ?");
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int index = 1;
            for (String param : params) {
                ps.setString(index++, param);
            }
            ps.setInt(index, Math.max(1, Math.min(limit, 1000)));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    MedicalDataRecord record = mapRecordHeader(rs);
                    record.setDeviceName(firstNonBlank(record.getDeviceName(), rs.getString("display_name")));
                    records.add(record);
                }
            }
            populateRecordSummaries(conn, records);
        } catch (SQLException ex) {
            logger.error("queryRecords failed", ex);
        }
        return records;
    }

    public MedicalDataRecord loadRecord(long headerId) {
        String headerSql = "SELECT h.*, r.raw_payload, r.parse_status, r.parse_error, COALESCE(h.device_name, d.nickname, d.device_id, h.device_id) AS display_name " +
            "FROM client_medical_data_header h " +
            "LEFT JOIN client_medical_data_raw r ON r.id=h.raw_id " +
            "LEFT JOIN client_device_registry d ON d.org_code=h.org_code AND d.device_id=h.device_id " +
            "WHERE h.id=?";
        String itemSql = "SELECT * FROM client_medical_data_item WHERE header_id=? ORDER BY sort_no, id";
        try (Connection conn = connect(); PreparedStatement headerPs = conn.prepareStatement(headerSql)) {
            headerPs.setLong(1, headerId);
            try (ResultSet rs = headerPs.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                MedicalDataRecord record = mapRecordHeader(rs);
                record.setDeviceName(firstNonBlank(record.getDeviceName(), rs.getString("display_name")));
                try (PreparedStatement itemPs = conn.prepareStatement(itemSql)) {
                    itemPs.setLong(1, headerId);
                    try (ResultSet itemRs = itemPs.executeQuery()) {
                        while (itemRs.next()) {
                            record.getItems().add(mapRecordItem(itemRs));
                        }
                    }
                }
                record.setSummaryText(VentilatorRecordSupport.buildSummary(record));
                return record;
            }
        } catch (SQLException ex) {
            logger.error("loadRecord failed", ex);
            return null;
        }
    }

    public int countTodayRecords(String orgCode) {
        String today = LocalDate.now().format(DateTimeFormatter.ISO_DATE) + "%";
        String sql = isBlank(orgCode)
            ? "SELECT COUNT(1) FROM client_medical_data_header WHERE receive_time LIKE ?"
            : "SELECT COUNT(1) FROM client_medical_data_header WHERE org_code=? AND receive_time LIKE ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            if (isBlank(orgCode)) {
                ps.setString(1, today);
            } else {
                ps.setString(1, orgCode.trim());
                ps.setString(2, today);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException ex) {
            logger.error("countTodayRecords failed", ex);
            return 0;
        }
    }

    public synchronized void saveExportTask(String orgCode,
                                            String deviceId,
                                            String exportType,
                                            LocalDateTime rangeFrom,
                                            LocalDateTime rangeTo,
                                            int recordCount,
                                            String filePath,
                                            String operatorName) {
        String sql = "INSERT INTO client_export_task(org_code, device_id, export_type, range_from, range_to, record_count, file_path, operator_name, created_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, defaultOrgCode(orgCode));
            ps.setString(2, blankToNull(deviceId));
            ps.setString(3, firstNonBlank(exportType, "CSV"));
            ps.setString(4, formatTime(rangeFrom));
            ps.setString(5, formatTime(rangeTo));
            ps.setInt(6, Math.max(recordCount, 0));
            ps.setString(7, blankToNull(filePath));
            ps.setString(8, blankToNull(operatorName));
            ps.setString(9, formatTime(LocalDateTime.now()));
            ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("saveExportTask failed", ex);
        }
    }

    private void saveHeaderAndItems(Connection conn,
                                    String orgCode,
                                    DeviceRegistry device,
                                    Hl7ResultRecord record,
                                    long rawId,
                                    String rawHash,
                                    String receiveTime,
                                    String parseStatus) throws SQLException {
        String deviceId = device == null ? buildAutoDeviceId(null, 0) : device.getDeviceId();
        String deviceType = device == null ? inferDeviceType(record) : firstNonBlank(device.getDeviceType(), inferDeviceType(record));
        String dataHash = sha256(rawHash + "|" + firstNonBlank(deviceId, "-") + "|" +
            firstNonBlank(record.getMessageControlId(), "-") + "|" + firstNonBlank(record.getSampleNo(), "-"));
        long headerId = findHeaderIdByHash(conn, dataHash);
        if (headerId > 0) {
            return;
        }

        String headerSql = "INSERT INTO client_medical_data_header(" +
            "raw_id, org_code, device_id, device_type, device_name, data_category, protocol_code, message_type, trigger_event, message_control_id, patient_id, patient_name, patient_gender, patient_age, " +
            "ward_name, bed_name, visit_id, sample_no, report_no, observation_time, report_time, receive_time, record_status, data_hash, summary_json, created_at, updated_at" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String itemSql = "INSERT INTO client_medical_data_item(" +
            "header_id, segment_type, item_code, item_name, value_type, value_text, numeric_value, unit, ref_range, abnormal_flag, observed_at, sort_no, ext_json, created_at" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String now = formatTime(LocalDateTime.now());
        try (PreparedStatement headerPs = conn.prepareStatement(headerSql, Statement.RETURN_GENERATED_KEYS)) {
            headerPs.setLong(1, rawId);
            headerPs.setString(2, orgCode);
            headerPs.setString(3, deviceId);
            headerPs.setString(4, firstNonBlank(deviceType, "UNKNOWN"));
            headerPs.setString(5, device == null ? null : blankToNull(device.getDisplayName()));
            headerPs.setString(6, firstNonBlank(record.getDataCategory(), inferDataCategory(deviceType)));
            headerPs.setString(7, "HL7V2_MLLP");
            headerPs.setString(8, blankToNull(record.getMessageType()));
            headerPs.setString(9, blankToNull(record.getTriggerEvent()));
            headerPs.setString(10, blankToNull(record.getMessageControlId()));
            headerPs.setString(11, blankToNull(firstNonBlank(record.getPatientId(), device == null ? null : device.getPatientId())));
            headerPs.setString(12, blankToNull(firstNonBlank(record.getPatientName(), device == null ? null : device.getPatientName())));
            headerPs.setString(13, blankToNull(record.getPatientGender()));
            headerPs.setString(14, blankToNull(record.getPatientAge()));
            headerPs.setString(15, blankToNull(firstNonBlank(record.getWardName(), device == null ? null : device.getWardName())));
            headerPs.setString(16, blankToNull(firstNonBlank(record.getBedName(), device == null ? null : device.getBedName())));
            headerPs.setString(17, blankToNull(device == null ? null : device.getAdmissionNo()));
            headerPs.setString(18, blankToNull(record.getSampleNo()));
            headerPs.setString(19, blankToNull(record.getHisRequestNo()));
            headerPs.setString(20, formatTime(firstObservationTime(record)));
            headerPs.setString(21, formatTime(record.getResultTime()));
            headerPs.setString(22, receiveTime);
            headerPs.setString(23, "SUCCESS".equals(parseStatus) ? "PARSED" : "PENDING_BIND");
            headerPs.setString(24, dataHash);
            headerPs.setString(25, "{\"itemCount\":" + record.getObservations().size() + "}");
            headerPs.setString(26, now);
            headerPs.setString(27, now);
            headerPs.executeUpdate();
            try (ResultSet rs = headerPs.getGeneratedKeys()) {
                if (rs.next()) {
                    headerId = rs.getLong(1);
                }
            }
        }

        if (headerId <= 0) {
            return;
        }
        try (PreparedStatement itemPs = conn.prepareStatement(itemSql)) {
            for (Hl7Observation observation : record.getObservations()) {
                itemPs.setLong(1, headerId);
                itemPs.setString(2, "OBX");
                itemPs.setString(3, blankToNull(observation.getItemCode()));
                itemPs.setString(4, blankToNull(observation.getItemName()));
                itemPs.setString(5, blankToNull(observation.getValueType()));
                itemPs.setString(6, blankToNull(observation.getValue()));
                if (observation.getNumericValue() != null) {
                    itemPs.setDouble(7, observation.getNumericValue());
                } else {
                    itemPs.setObject(7, null);
                }
                itemPs.setString(8, blankToNull(observation.getUnit()));
                itemPs.setString(9, blankToNull(observation.getReferenceRange()));
                itemPs.setString(10, blankToNull(observation.getAbnormalFlag()));
                itemPs.setString(11, formatTime(observation.getObservedAt()));
                itemPs.setInt(12, observation.getSortNo());
                itemPs.setObject(13, null);
                itemPs.setString(14, now);
                itemPs.addBatch();
            }
            itemPs.executeBatch();
        }
    }

    private void populateRecordSummaries(Connection conn, List<MedicalDataRecord> records) throws SQLException {
        if (records == null || records.isEmpty()) {
            return;
        }
        Map<Long, MedicalDataRecord> recordMap = new LinkedHashMap<>();
        for (MedicalDataRecord record : records) {
            if (record == null || record.getId() <= 0 || !VentilatorRecordSupport.isVentilatorRecord(record)) {
                continue;
            }
            recordMap.put(record.getId(), record);
        }
        if (recordMap.isEmpty()) {
            return;
        }

        StringBuilder sql = new StringBuilder("SELECT * FROM client_medical_data_item WHERE header_id IN (");
        boolean first = true;
        for (int i = 0; i < recordMap.size(); i++) {
            if (!first) {
                sql.append(',');
            }
            first = false;
            sql.append('?');
        }
        sql.append(") ORDER BY header_id, sort_no, id");

        Map<Long, List<MedicalDataItem>> itemMap = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int index = 1;
            for (Long headerId : recordMap.keySet()) {
                ps.setLong(index++, headerId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long headerId = rs.getLong("header_id");
                    itemMap.computeIfAbsent(headerId, ignored -> new ArrayList<>()).add(mapRecordItem(rs));
                }
            }
        }
        for (Map.Entry<Long, MedicalDataRecord> entry : recordMap.entrySet()) {
            List<MedicalDataItem> items = itemMap.get(entry.getKey());
            entry.getValue().setSummaryText(VentilatorRecordSupport.buildSummary(items));
        }
    }

    private long findRawIdByHash(Connection conn, String rawHash) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM client_medical_data_raw WHERE raw_hash=? LIMIT 1")) {
            ps.setString(1, rawHash);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1L;
            }
        }
    }

    private long findHeaderIdByHash(Connection conn, String dataHash) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM client_medical_data_header WHERE data_hash=? LIMIT 1")) {
            ps.setString(1, dataHash);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1L;
            }
        }
    }

    private DeviceRegistry selectDeviceByRoute(Connection conn, String orgCode, int listenPort, String sourceIp) throws SQLException {
        if (isBlank(sourceIp) || listenPort <= 0) {
            return null;
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT * FROM client_device_registry WHERE org_code=? AND listen_port=? AND source_ip=? ORDER BY enabled DESC, updated_at DESC LIMIT 1")) {
            ps.setString(1, orgCode);
            ps.setInt(2, listenPort);
            ps.setString(3, sourceIp.trim());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? mapDevice(rs) : null;
            }
        }
    }

    private DeviceRegistry selectDeviceByOrgAndId(Connection conn, String orgCode, String deviceId) throws SQLException {
        if (isBlank(orgCode) || isBlank(deviceId)) {
            return null;
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT * FROM client_device_registry WHERE org_code=? AND device_id=? LIMIT 1")) {
            ps.setString(1, orgCode.trim());
            ps.setString(2, deviceId.trim());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? mapDevice(rs) : null;
            }
        }
    }

    private void updateBindingHistory(Connection conn, DeviceRegistry device, String now) throws SQLException {
        if (device == null || isBlank(device.getDeviceId()) || isBlank(device.getOrgCode())) {
            return;
        }
        boolean hasBinding = !isBlank(device.getPatientId())
            || !isBlank(device.getPatientName())
            || !isBlank(device.getPatientNo())
            || !isBlank(device.getAdmissionNo());
        if (!hasBinding) {
            return;
        }
        String latestSql = "SELECT id, patient_id, patient_name, patient_no, admission_no, ward_name, bed_name FROM client_device_patient_binding " +
            "WHERE org_code=? AND device_id=? AND expired_at IS NULL ORDER BY id DESC LIMIT 1";
        try (PreparedStatement latestPs = conn.prepareStatement(latestSql)) {
            latestPs.setString(1, device.getOrgCode());
            latestPs.setString(2, device.getDeviceId());
            try (ResultSet rs = latestPs.executeQuery()) {
                if (rs.next()) {
                    boolean same = equalsText(rs.getString("patient_id"), device.getPatientId())
                        && equalsText(rs.getString("patient_name"), device.getPatientName())
                        && equalsText(rs.getString("patient_no"), device.getPatientNo())
                        && equalsText(rs.getString("admission_no"), device.getAdmissionNo())
                        && equalsText(rs.getString("ward_name"), device.getWardName())
                        && equalsText(rs.getString("bed_name"), device.getBedName());
                    if (same) {
                        return;
                    }
                    try (PreparedStatement closePs = conn.prepareStatement(
                        "UPDATE client_device_patient_binding SET expired_at=? WHERE id=?")) {
                        closePs.setString(1, now);
                        closePs.setLong(2, rs.getLong("id"));
                        closePs.executeUpdate();
                    }
                }
            }
        }
        try (PreparedStatement insertPs = conn.prepareStatement(
            "INSERT INTO client_device_patient_binding(org_code, device_id, patient_id, patient_name, patient_no, admission_no, ward_name, bed_name, bind_source, effective_at, created_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            insertPs.setString(1, device.getOrgCode());
            insertPs.setString(2, device.getDeviceId());
            insertPs.setString(3, blankToNull(device.getPatientId()));
            insertPs.setString(4, blankToNull(device.getPatientName()));
            insertPs.setString(5, blankToNull(device.getPatientNo()));
            insertPs.setString(6, blankToNull(device.getAdmissionNo()));
            insertPs.setString(7, blankToNull(device.getWardName()));
            insertPs.setString(8, blankToNull(device.getBedName()));
            insertPs.setString(9, firstNonBlank(device.getBindSource(), "LOCAL"));
            insertPs.setString(10, now);
            insertPs.setString(11, now);
            insertPs.executeUpdate();
        }
    }

    private DeviceRegistry buildAutoDevice(String orgCode,
                                           int listenPort,
                                           String sourceIp,
                                           int sourcePort,
                                           Hl7ResultRecord record) {
        DeviceRegistry device = new DeviceRegistry();
        device.setOrgCode(defaultOrgCode(orgCode));
        device.setDeviceType(inferDeviceType(record));
        device.setProtocolCode("HL7V2_MLLP");
        device.setDeviceId(buildAutoDeviceId(sourceIp, listenPort));
        device.setSourceIp(blankToNull(sourceIp));
        device.setSourcePort(sourcePort);
        device.setListenPort(listenPort > 0 ? listenPort : 5555);
        device.setNickname("待绑定设备-" + firstNonBlank(sourceIp, "unknown"));
        if (record != null) {
            device.setWardName(record.getWardName());
            device.setBedName(record.getBedName());
            device.setPatientId(record.getPatientId());
            device.setPatientName(record.getPatientName());
            device.setBindSource("HL7_SUGGESTED");
            device.setExtJson("{\"sourceApplication\":\"" + safeJson(record.getSourceApplication()) +
                "\",\"sourceFacility\":\"" + safeJson(record.getSourceFacility()) + "\"}");
        } else {
            device.setBindSource("LOCAL");
        }
        device.setAckMode("OPTIONAL");
        device.setEnabled(true);
        device.setLastSeenAt(LocalDateTime.now());
        return device;
    }

    private String buildAutoDeviceId(String sourceIp, int listenPort) {
        return "AUTO-" + firstNonBlank(blankToNull(sourceIp), "UNKNOWN").replace(":", "_") + "-" + Math.max(listenPort, 0);
    }

    private String inferDeviceType(Hl7ResultRecord record) {
        if (record == null) {
            return "UNKNOWN";
        }
        StringBuilder sb = new StringBuilder();
        if (!isBlank(record.getSourceApplication())) {
            sb.append(record.getSourceApplication()).append(' ');
        }
        if (!isBlank(record.getSourceFacility())) {
            sb.append(record.getSourceFacility()).append(' ');
        }
        for (Hl7Observation observation : record.getObservations()) {
            if (!isBlank(observation.getItemCode())) {
                sb.append(observation.getItemCode()).append(' ');
            }
            if (!isBlank(observation.getItemName())) {
                sb.append(observation.getItemName()).append(' ');
            }
        }
        String text = sb.toString().toUpperCase();
        if (text.contains("PEEP") || text.contains("FIO2") || text.contains("VT") || text.contains("MV")) {
            return "VENTILATOR";
        }
        if (text.contains("ECG") || text.contains("SPO2") || text.contains("NIBP") || text.contains("HEART")) {
            return "MONITOR";
        }
        if (text.contains("ULTRASOUND") || text.contains("US")) {
            return "ULTRASOUND";
        }
        if (text.contains("CT")) {
            return "CT";
        }
        return "UNKNOWN";
    }

    private String inferDataCategory(String deviceType) {
        String type = firstNonBlank(deviceType, "UNKNOWN").toUpperCase();
        if ("ULTRASOUND".equals(type) || "CT".equals(type)) {
            return "IMAGE_META";
        }
        return "VITAL_SIGN";
    }

    private LocalDateTime firstObservationTime(Hl7ResultRecord record) {
        if (record == null || record.getObservations() == null || record.getObservations().isEmpty()) {
            return record == null ? null : record.getResultTime();
        }
        for (Hl7Observation observation : record.getObservations()) {
            if (observation.getObservedAt() != null) {
                return observation.getObservedAt();
            }
        }
        return record.getResultTime();
    }

    private OrgInfo mapOrgInfo(ResultSet rs) throws SQLException {
        OrgInfo orgInfo = new OrgInfo();
        orgInfo.setOrgCode(rs.getString("org_code"));
        orgInfo.setOrgName(rs.getString("org_name"));
        orgInfo.setHisCode(rs.getString("his_code"));
        orgInfo.setSiOrgCode(rs.getString("si_org_code"));
        orgInfo.setWebserviceUrl(rs.getString("webservice_url"));
        orgInfo.setSsoAccount(rs.getString("sso_account"));
        orgInfo.setOperId(rs.getString("oper_id"));
        orgInfo.setRealName(rs.getString("real_name"));
        orgInfo.setTokenSnapshot(rs.getString("token_snapshot"));
        orgInfo.setLastLoginAt(parseTime(rs.getString("last_login_at")));
        return orgInfo;
    }

    private DeviceRegistry mapDevice(ResultSet rs) throws SQLException {
        DeviceRegistry device = new DeviceRegistry();
        device.setId(rs.getLong("id"));
        device.setOrgCode(rs.getString("org_code"));
        device.setDeviceType(rs.getString("device_type"));
        device.setProtocolCode(rs.getString("protocol_code"));
        device.setVendorName(rs.getString("vendor_name"));
        device.setModelName(rs.getString("model_name"));
        device.setDeviceId(rs.getString("device_id"));
        device.setMacAddress(rs.getString("mac_address"));
        device.setSourceIp(rs.getString("source_ip"));
        device.setSourcePort(rs.getInt("source_port"));
        device.setListenPort(rs.getInt("listen_port"));
        device.setNickname(rs.getString("nickname"));
        device.setWardCode(rs.getString("ward_code"));
        device.setWardName(rs.getString("ward_name"));
        device.setBedCode(rs.getString("bed_code"));
        device.setBedName(rs.getString("bed_name"));
        device.setPatientId(rs.getString("patient_id"));
        device.setPatientName(rs.getString("patient_name"));
        device.setPatientNo(rs.getString("patient_no"));
        device.setAdmissionNo(rs.getString("admission_no"));
        device.setBindSource(rs.getString("bind_source"));
        device.setAckMode(rs.getString("ack_mode"));
        device.setEnabled(rs.getInt("enabled") == 1);
        device.setLastSeenAt(parseTime(rs.getString("last_seen_at")));
        device.setExtJson(rs.getString("ext_json"));
        return device;
    }

    private MedicalDataRecord mapRecordHeader(ResultSet rs) throws SQLException {
        MedicalDataRecord record = new MedicalDataRecord();
        record.setId(rs.getLong("id"));
        record.setRawId(rs.getLong("raw_id"));
        record.setOrgCode(rs.getString("org_code"));
        record.setDeviceId(rs.getString("device_id"));
        record.setDeviceName(rs.getString("device_name"));
        record.setDeviceType(rs.getString("device_type"));
        record.setDataCategory(rs.getString("data_category"));
        record.setProtocolCode(rs.getString("protocol_code"));
        record.setMessageType(rs.getString("message_type"));
        record.setTriggerEvent(rs.getString("trigger_event"));
        record.setMessageControlId(rs.getString("message_control_id"));
        record.setPatientId(rs.getString("patient_id"));
        record.setPatientName(rs.getString("patient_name"));
        record.setPatientGender(rs.getString("patient_gender"));
        record.setPatientAge(rs.getString("patient_age"));
        record.setWardName(rs.getString("ward_name"));
        record.setBedName(rs.getString("bed_name"));
        record.setVisitId(rs.getString("visit_id"));
        record.setSampleNo(rs.getString("sample_no"));
        record.setReportNo(rs.getString("report_no"));
        record.setObservationTime(parseTime(rs.getString("observation_time")));
        record.setReportTime(parseTime(rs.getString("report_time")));
        record.setReceiveTime(parseTime(rs.getString("receive_time")));
        record.setRecordStatus(rs.getString("record_status"));
        record.setParseStatus(rs.getString("parse_status"));
        record.setParseError(rs.getString("parse_error"));
        record.setRawPayload(rs.getString("raw_payload"));
        return record;
    }

    private MedicalDataItem mapRecordItem(ResultSet rs) throws SQLException {
        MedicalDataItem item = new MedicalDataItem();
        item.setId(rs.getLong("id"));
        item.setHeaderId(rs.getLong("header_id"));
        item.setSegmentType(rs.getString("segment_type"));
        item.setItemCode(rs.getString("item_code"));
        item.setItemName(rs.getString("item_name"));
        item.setValueType(rs.getString("value_type"));
        item.setValueText(rs.getString("value_text"));
        double numericValue = rs.getDouble("numeric_value");
        item.setNumericValue(rs.wasNull() ? null : numericValue);
        item.setUnit(rs.getString("unit"));
        item.setRefRange(rs.getString("ref_range"));
        item.setAbnormalFlag(rs.getString("abnormal_flag"));
        item.setObservedAt(parseTime(rs.getString("observed_at")));
        item.setSortNo(rs.getInt("sort_no"));
        item.setExtJson(rs.getString("ext_json"));
        return item;
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    private String resolveDbPath(String configuredPath) {
        String path = isBlank(configuredPath) ? "./data/inmehl7-client.db" : configuredPath.trim();
        Path dbPath = Paths.get(path);
        if (!dbPath.isAbsolute()) {
            dbPath = Paths.get("").toAbsolutePath().resolve(dbPath).normalize();
        }
        try {
            Path parent = dbPath.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }
        } catch (Exception ex) {
            logger.warn("Failed to prepare db directory: " + ex.getMessage());
        }
        return dbPath.toString();
    }

    private String defaultOrgCode(String orgCode) {
        return firstNonBlank(blankToNull(orgCode), blankToNull(config.get("orgCode")), "DEFAULT");
    }

    private String formatTime(LocalDateTime value) {
        return value == null ? null : TS.format(value);
    }

    private LocalDateTime parseTime(String value) {
        if (isBlank(value)) {
            return null;
        }
        try {
            return LocalDateTime.parse(value.trim(), TS);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String sha256(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest((text == null ? "" : text).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception ex) {
            return Integer.toHexString((text == null ? "" : text).hashCode());
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private String blankToNull(String value) {
        return isBlank(value) ? null : value.trim();
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value.trim();
            }
        }
        return null;
    }

    private boolean equalsText(String left, String right) {
        String l = blankToNull(left);
        String r = blankToNull(right);
        return l == null ? r == null : l.equals(r);
    }

    private String safeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
