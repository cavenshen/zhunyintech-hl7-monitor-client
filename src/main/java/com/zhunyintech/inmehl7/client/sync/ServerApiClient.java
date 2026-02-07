package com.zhunyintech.inmehl7.client.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public class ServerApiClient {

    private final Config config;
    private final RuntimeLogger logger;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    public ServerApiClient(Config config, RuntimeLogger logger) {
        this.config = config;
        this.logger = logger;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public JsonNode refreshAuth(String token, Map<String, Object> body) throws IOException, InterruptedException {
        return post("/auth/refresh", token, body);
    }

    public JsonNode heartbeat(String token, Map<String, Object> body) throws IOException, InterruptedException {
        return post("/heartbeat", token, body);
    }

    public JsonNode syncResults(String token, Map<String, Object> body) throws IOException, InterruptedException {
        return post("/results/batch", token, body);
    }

    public JsonNode syncLogs(String token, Map<String, Object> body) throws IOException, InterruptedException {
        return post("/logs/batch", token, body);
    }

    public JsonNode pullConfig(String token) throws IOException, InterruptedException {
        return get("/config/pull", token);
    }

    private JsonNode post(String path, String token, Map<String, Object> body) throws IOException, InterruptedException {
        String url = baseUrl() + path;
        String json = mapper.writeValueAsString(body);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json));
        setTokenHeader(requestBuilder, token);

        HttpResponse<String> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("HTTP " + response.statusCode() + " " + response.body());
        }
        return unwrap(response.body());
    }

    private JsonNode get(String path, String token) throws IOException, InterruptedException {
        String url = baseUrl() + path;
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .GET();
        setTokenHeader(requestBuilder, token);

        HttpResponse<String> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("HTTP " + response.statusCode() + " " + response.body());
        }
        return unwrap(response.body());
    }

    private JsonNode unwrap(String responseBody) throws IOException {
        JsonNode node = mapper.readTree(responseBody);
        JsonNode codeNode = node.get("code");
        if (codeNode != null && codeNode.asInt(-1) != 0) {
            String message = node.has("message") ? node.get("message").asText() : "unknown error";
            throw new IOException("Server error: " + message);
        }
        JsonNode data = node.get("data");
        if (data == null) {
            return mapper.nullNode();
        }
        return data;
    }

    private void setTokenHeader(HttpRequest.Builder builder, String token) {
        if (token == null || token.trim().isEmpty()) {
            return;
        }
        String headerName = config.get("authHeaderName");
        if (headerName == null || headerName.trim().isEmpty()) {
            headerName = "x-auth-token";
        }
        builder.header(headerName, token.trim());
    }

    private String baseUrl() {
        String base = config.get("serverClientApiBase");
        if (base == null || base.trim().isEmpty()) {
            base = "http://127.0.0.1:9019/api/inmehl7/v1/client";
        }
        base = base.trim();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base;
    }
}

