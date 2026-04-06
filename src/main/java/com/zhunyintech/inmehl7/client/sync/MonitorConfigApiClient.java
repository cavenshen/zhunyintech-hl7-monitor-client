package com.zhunyintech.inmehl7.client.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.MonitorListenerProfile;
import com.zhunyintech.inmehl7.client.model.MonitorProtocolType;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MonitorConfigApiClient {

    private final Config config;
    private final RuntimeLogger logger;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    public MonitorConfigApiClient(Config config, RuntimeLogger logger) {
        this.config = config;
        this.logger = logger;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .version(HttpClient.Version.HTTP_1_1)
            .build();
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public List<MonitorListenerProfile> listProfiles(String token, String macAddress) throws IOException, InterruptedException {
        if (macAddress == null || macAddress.trim().isEmpty()) {
            return Collections.emptyList();
        }
        String url = baseUrl() + "/listener-configs?macAddress=" + URLEncoder.encode(macAddress, StandardCharsets.UTF_8);
        JsonNode data = get(url, token);
        if (data == null || !data.isArray()) {
            return Collections.emptyList();
        }
        List<MonitorListenerProfile> items = new ArrayList<>();
        for (JsonNode node : data) {
            items.add(mapProfile(node));
        }
        return items;
    }

    public MonitorListenerProfile saveProfile(String token, MonitorListenerProfile profile) throws IOException, InterruptedException {
        if (profile == null) {
            return null;
        }
        String url = baseUrl() + "/listener-config";
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("clientCode", profile.getClientCode());
        body.put("macAddress", profile.getMacAddress());
        body.put("protocolType", profile.getProtocolType().getCode());
        body.put("protocolName", profile.getProtocolType().getDisplayName());
        body.put("orgCode", profile.getOrgCode());
        body.put("orgName", profile.getOrgName());
        body.put("exportDir", profile.getExportDir());
        body.put("syncIntervalSec", profile.getSyncIntervalSec());
        body.put("heartbeatIntervalSec", profile.getHeartbeatIntervalSec());
        body.put("listenPort", profile.getListenPort());
        body.put("serialPortName", profile.getSerialPortName());
        body.put("baudRate", profile.getBaudRate());
        body.put("dataBits", profile.getDataBits());
        body.put("stopBits", profile.getStopBits());
        body.put("parity", profile.getParity());
        body.put("readTimeoutMs", profile.getReadTimeoutMs());
        body.put("pollIntervalMs", profile.getPollIntervalMs());
        body.put("charsetName", profile.getCharsetName());
        body.put("frameDelimiter", profile.getFrameDelimiter());
        body.put("stationNo", profile.getStationNo());
        body.put("extJson", profile.getExtJson());
        JsonNode data = post(url, token, body);
        return data == null || data.isNull() ? null : mapProfile(data);
    }

    private MonitorListenerProfile mapProfile(JsonNode node) {
        MonitorListenerProfile profile = new MonitorListenerProfile();
        profile.setId(node.path("id").isMissingNode() || node.path("id").isNull() ? null : node.path("id").asLong());
        profile.setClientCode(text(node, "clientCode"));
        profile.setMacAddress(text(node, "macAddress"));
        profile.setProtocolType(MonitorProtocolType.of(text(node, "protocolType")));
        profile.setOrgCode(text(node, "orgCode"));
        profile.setOrgName(text(node, "orgName"));
        profile.setExportDir(text(node, "exportDir"));
        profile.setSyncIntervalSec(node.path("syncIntervalSec").asInt(60));
        profile.setHeartbeatIntervalSec(node.path("heartbeatIntervalSec").asInt(60));
        profile.setListenPort(node.path("listenPort").isMissingNode() || node.path("listenPort").isNull() ? null : node.path("listenPort").asInt());
        profile.setSerialPortName(text(node, "serialPortName"));
        profile.setBaudRate(node.path("baudRate").isMissingNode() || node.path("baudRate").isNull() ? null : node.path("baudRate").asInt());
        profile.setDataBits(node.path("dataBits").isMissingNode() || node.path("dataBits").isNull() ? null : node.path("dataBits").asInt());
        profile.setStopBits(node.path("stopBits").isMissingNode() || node.path("stopBits").isNull() ? null : node.path("stopBits").asInt());
        profile.setParity(text(node, "parity"));
        profile.setReadTimeoutMs(node.path("readTimeoutMs").isMissingNode() || node.path("readTimeoutMs").isNull() ? null : node.path("readTimeoutMs").asInt());
        profile.setPollIntervalMs(node.path("pollIntervalMs").isMissingNode() || node.path("pollIntervalMs").isNull() ? null : node.path("pollIntervalMs").asInt());
        profile.setCharsetName(text(node, "charsetName"));
        profile.setFrameDelimiter(text(node, "frameDelimiter"));
        profile.setStationNo(text(node, "stationNo"));
        profile.setExtJson(text(node, "extJson"));
        return profile;
    }

    private JsonNode post(String url, String token, Map<String, Object> body) throws IOException, InterruptedException {
        String json = mapper.writeValueAsString(body);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json));
        setTokenHeader(requestBuilder, token);
        logger.info("HTTP POST " + url);
        HttpResponse<String> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        logger.info("HTTP POST " + url + " -> " + response.statusCode());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("POST " + url + " -> HTTP " + response.statusCode() + " " + response.body());
        }
        return unwrap(response.body(), "POST " + url);
    }

    private JsonNode get(String url, String token) throws IOException, InterruptedException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .GET();
        setTokenHeader(requestBuilder, token);
        logger.info("HTTP GET " + url);
        HttpResponse<String> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        logger.info("HTTP GET " + url + " -> " + response.statusCode());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("GET " + url + " -> HTTP " + response.statusCode() + " " + response.body());
        }
        return unwrap(response.body(), "GET " + url);
    }

    private JsonNode unwrap(String responseBody, String context) throws IOException {
        JsonNode node = mapper.readTree(responseBody);
        JsonNode codeNode = node.get("code");
        if (codeNode != null && codeNode.asInt(-1) != 0) {
            throw new IOException(context + " -> Server error: " + text(node, "message"));
        }
        JsonNode data = node.get("data");
        return data == null ? mapper.nullNode() : data;
    }

    private void setTokenHeader(HttpRequest.Builder builder, String token) {
        if (token == null || token.trim().isEmpty()) {
            return;
        }
        String tokenValue = token.trim();
        String mode = resolveAuthHeaderMode(tokenValue);
        String headerName = config.get("authHeaderName");
        if (headerName == null || headerName.trim().isEmpty()) {
            headerName = "x-auth-token";
        }
        if ("both".equals(mode)) {
            builder.header("Authorization", "Bearer " + tokenValue);
            builder.header(headerName, tokenValue);
            if (!"x-auth-token".equalsIgnoreCase(headerName)) {
                builder.header("x-auth-token", tokenValue);
            }
            return;
        }
        if ("authorization".equals(mode) || "bearer".equals(mode)) {
            builder.header("Authorization", "Bearer " + tokenValue);
            return;
        }
        builder.header(headerName, tokenValue);
    }

    private String resolveAuthHeaderMode(String tokenValue) {
        String mode = config.get("authHeaderMode");
        String normalized = mode == null ? "" : mode.trim().toLowerCase();
        if (normalized.isEmpty() || "auto".equals(normalized)) {
            return looksLikeJwt(tokenValue) ? "authorization" : "x-auth-token";
        }
        if ("x_auth_token".equals(normalized) || "xauthtoken".equals(normalized)) {
            return "x-auth-token";
        }
        return normalized;
    }

    private boolean looksLikeJwt(String tokenValue) {
        int firstDot = tokenValue.indexOf('.');
        return firstDot > 0 && tokenValue.indexOf('.', firstDot + 1) > firstDot + 1;
    }

    private String text(JsonNode node, String field) {
        if (node == null || field == null || node.get(field) == null || node.get(field).isNull()) {
            return null;
        }
        String value = node.get(field).asText();
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private String baseUrl() {
        String base = config.get("clientConfigApiBase");
        if (base == null || base.trim().isEmpty()) {
            base = "http://127.0.0.1:9016/api/zyhis/v1/client/config/hl7-monitor";
        }
        base = base.trim();
        return base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
    }
}
