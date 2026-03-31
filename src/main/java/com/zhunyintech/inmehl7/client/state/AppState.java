package com.zhunyintech.inmehl7.client.state;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.db.ClientRepository;
import com.zhunyintech.inmehl7.client.db.SQLiteStore;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.LicenseSnapshot;
import com.zhunyintech.inmehl7.client.model.OrgInfo;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.awt.Desktop;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class AppState {

    private final Config config;
    private final SQLiteStore store;
    private final ClientRepository repository;
    private final RuntimeLogger logger;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    private volatile String token;
    private volatile LicenseSnapshot licenseSnapshot = new LicenseSnapshot();
    private volatile OrgInfo currentOrgInfo;

    public AppState(Config config, SQLiteStore store, ClientRepository repository, RuntimeLogger logger) {
        this.config = config;
        this.store = store;
        this.repository = repository;
        this.logger = logger;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.mapper = new ObjectMapper();
        this.token = store.loadAuthToken();
        this.currentOrgInfo = repository.loadOrgInfo();
        if (currentOrgInfo != null && !isBlank(currentOrgInfo.getOrgCode())) {
            config.set("orgCode", currentOrgInfo.getOrgCode());
        }
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = isBlank(token) ? null : token.trim();
        if (!isBlank(this.token)) {
            store.saveAuthToken(this.token);
        }
    }

    public LicenseSnapshot getLicenseSnapshot() {
        return licenseSnapshot;
    }

    public void setLicenseSnapshot(LicenseSnapshot licenseSnapshot) {
        if (licenseSnapshot != null) {
            this.licenseSnapshot = licenseSnapshot;
        }
    }

    public OrgInfo getCurrentOrgInfo() {
        return currentOrgInfo;
    }

    public String getCurrentOrgCode() {
        return firstNonBlank(
            currentOrgInfo == null ? null : currentOrgInfo.getOrgCode(),
            config.get("orgCode"),
            "DEFAULT"
        );
    }

    public boolean refreshCurrentOrg(Consumer<String> statusCallback) {
        if (isBlank(token)) {
            notify(statusCallback, "未登录，暂无法刷新机构信息");
            return false;
        }
        try {
            initializeCurrentOrg(statusCallback);
            return currentOrgInfo != null;
        } catch (Exception ex) {
            logger.error("refreshCurrentOrg failed", ex);
            notify(statusCallback, "刷新机构信息失败: " + ex.getMessage());
            return false;
        }
    }

    public boolean loginInBrowser(Consumer<String> statusCallback) {
        String ssoBaseUrl = trimTrailingSlash(config.get("ssoBaseUrl"));
        String ssoAppId = valueOrDefault(config.get("ssoAppId"), "inme-hl7");
        int callbackPort = config.getInt("ssoCallbackPort", 17654);
        int timeoutSec = config.getInt("ssoCallbackTimeoutSec", 180);
        String callbackUrl = "http://127.0.0.1:" + callbackPort + "/";
        String loginUrl = buildBrowserLoginUrl(ssoBaseUrl, ssoAppId, callbackUrl);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> tokenRef = new AtomicReference<>();
        HttpServer server = null;
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", callbackPort), 0);
            HttpServer finalServer = server;
            server.createContext("/", exchange -> {
                handleCallback(exchange, tokenRef, latch);
                finalServer.stop(0);
            });
            server.setExecutor(null);
            server.start();

            notify(statusCallback, "请在浏览器完成 SSO 登录: " + loginUrl);
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().browse(URI.create(loginUrl));
            }

            boolean ok = latch.await(timeoutSec, TimeUnit.SECONDS);
            if (!ok) {
                notify(statusCallback, "SSO 登录回调超时");
                return false;
            }
            String receivedToken = tokenRef.get();
            if (isBlank(receivedToken)) {
                notify(statusCallback, "未获取到 token");
                return false;
            }
            setToken(receivedToken.trim());
            notify(statusCallback, "SSO 登录成功，开始初始化机构信息");
            try {
                initializeCurrentOrg(statusCallback);
            } catch (Exception ex) {
                logger.error("initializeCurrentOrg failed", ex);
                notify(statusCallback, "机构信息初始化失败，但 token 已保存: " + ex.getMessage());
            }
            return true;
        } catch (Exception ex) {
            logger.error("loginInBrowser failed", ex);
            notify(statusCallback, "SSO 登录失败: " + ex.getMessage());
            return false;
        } finally {
            if (server != null) {
                try {
                    server.stop(0);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private void initializeCurrentOrg(Consumer<String> statusCallback) throws IOException, InterruptedException {
        String ssoBaseUrl = trimTrailingSlash(config.get("ssoBaseUrl"));
        String ssoAppId = valueOrDefault(config.get("ssoAppId"), "inme-hl7");

        JsonNode ssoProfile = getJsonSafe(resolveSsoProfileUrl(ssoBaseUrl, ssoAppId), "SSO 个人信息", statusCallback);
        JsonNode authMe = getJsonSafe(resolveAppBaseUrl() + valueOrDefault(config.get("appAuthMePath"), "/api/zyhisplus/v1/auth/me"), "业务端个人信息", statusCallback);
        JsonNode authOrgs = getJsonSafe(resolveAppBaseUrl() + valueOrDefault(config.get("appAuthOrgsPath"), "/api/zyhisplus/v1/auth/orgs"), "业务端机构列表", statusCallback);

        String orgCode = firstNonBlank(
            text(authMe, "orgCode", "orgcode"),
            text(ssoProfile, "orgCode", "orgcode"),
            text(firstArrayObject(authOrgs), "orgCode", "orgcode"),
            config.get("orgCode")
        );
        String orgInfoPath = valueOrDefault(config.get("orgInfoPath"), "/api/zyhisplus/v1/phisstockinvi/getPaykindInfo/{orgCode}");
        JsonNode orgNode = null;
        if (!isBlank(orgCode)) {
            String url = resolvePhisBaseUrl() + orgInfoPath.replace("{orgCode}", urlEncode(orgCode));
            orgNode = getJsonSafe(url, "机构详情", statusCallback);
        }

        OrgInfo orgInfo = mergeOrgInfo(orgCode, ssoProfile, authMe, firstArrayObject(authOrgs), orgNode);
        if (orgInfo == null || isBlank(orgInfo.getOrgCode())) {
            throw new IOException("未解析到机构编码");
        }
        repository.saveOrgInfo(orgInfo);
        currentOrgInfo = orgInfo;
        config.set("orgCode", orgInfo.getOrgCode());
        notify(statusCallback, "机构初始化完成: " + orgInfo.getOrgCode() + " / " + firstNonBlank(orgInfo.getOrgName(), "-"));
    }

    private OrgInfo mergeOrgInfo(String orgCode, JsonNode ssoProfile, JsonNode authMe, JsonNode authOrg, JsonNode orgNode) {
        String resolvedOrgCode = firstNonBlank(
            orgCode,
            text(authMe, "orgCode", "orgcode"),
            text(ssoProfile, "orgCode", "orgcode"),
            text(authOrg, "orgCode", "orgcode")
        );
        if (isBlank(resolvedOrgCode)) {
            return null;
        }
        OrgInfo orgInfo = new OrgInfo();
        orgInfo.setOrgCode(resolvedOrgCode.trim());
        orgInfo.setOrgName(firstNonBlank(
            text(orgNode, "orgName", "orgname", "name", "paykindName"),
            text(authMe, "orgName", "orgname"),
            text(ssoProfile, "orgName", "departmentName", "orgname"),
            text(authOrg, "orgName", "orgname")
        ));
        orgInfo.setHisCode(firstNonBlank(text(orgNode, "hisCode", "hiscode"), text(authMe, "hisCode", "hiscode")));
        orgInfo.setSiOrgCode(firstNonBlank(text(orgNode, "siOrgCode", "siorgcode"), text(authMe, "siOrgCode", "siorgcode")));
        orgInfo.setWebserviceUrl(firstNonBlank(
            text(orgNode, "webserviceUrl", "frontEndIp", "frontendIp", "wsUrl"),
            text(authMe, "webserviceUrl", "frontEndIp")
        ));
        orgInfo.setSsoAccount(firstNonBlank(text(ssoProfile, "username", "account"), text(authMe, "username", "account")));
        orgInfo.setOperId(firstNonBlank(text(authMe, "operid", "operId"), text(ssoProfile, "operid", "operId")));
        orgInfo.setRealName(firstNonBlank(text(authMe, "realName", "realname", "name"), text(ssoProfile, "realName", "realname", "name")));
        orgInfo.setTokenSnapshot(maskToken(token));
        orgInfo.setLastLoginAt(LocalDateTime.now());
        return orgInfo;
    }

    private JsonNode getJson(String url) throws IOException, InterruptedException {
        if (isBlank(url)) {
            return mapper.nullNode();
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .header("accept", "application/json, */*");
        setTokenHeaders(builder, token);
        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("HTTP " + response.statusCode() + " " + response.body());
        }
        return unwrap(response.body());
    }

    private JsonNode unwrap(String responseBody) throws IOException {
        if (isBlank(responseBody)) {
            return mapper.nullNode();
        }
        JsonNode node = mapper.readTree(responseBody);
        JsonNode codeNode = node.get("code");
        if (codeNode != null && codeNode.isInt() && codeNode.asInt(0) != 0) {
            String message = text(node, "message", "msg");
            throw new IOException(firstNonBlank(message, "接口返回失败"));
        }
        JsonNode data = node.get("data");
        return data == null ? node : data;
    }

    private JsonNode getJsonSafe(String url, String label, Consumer<String> statusCallback) {
        try {
            return getJson(url);
        } catch (Exception ex) {
            notify(statusCallback, label + "获取失败: " + ex.getMessage());
            return mapper.nullNode();
        }
    }

    private void setTokenHeaders(HttpRequest.Builder builder, String token) {
        if (builder == null || isBlank(token)) {
            return;
        }
        builder.header("x-auth-token", token.trim());
        builder.header("Authorization", "Bearer " + token.trim());
        String headerName = valueOrDefault(config.get("authHeaderName"), "x-auth-token");
        if (!"x-auth-token".equalsIgnoreCase(headerName)) {
            builder.header(headerName, token.trim());
        }
    }

    private JsonNode firstArrayObject(JsonNode node) {
        if (node != null && node.isArray() && node.size() > 0) {
            return node.get(0);
        }
        return mapper.nullNode();
    }

    private String text(JsonNode node, String... fields) {
        if (node == null || node.isNull() || fields == null) {
            return null;
        }
        for (String field : fields) {
            if (field == null || field.isEmpty()) {
                continue;
            }
            JsonNode valueNode = node.get(field);
            if (valueNode != null && !valueNode.isNull()) {
                String text = valueNode.asText();
                if (!isBlank(text)) {
                    return text.trim();
                }
            }
        }
        return null;
    }

    private void handleCallback(HttpExchange exchange, AtomicReference<String> tokenRef, CountDownLatch latch) throws IOException {
        String query = exchange.getRequestURI() == null ? null : exchange.getRequestURI().getRawQuery();
        Map<String, String> params = parseQuery(query);
        tokenRef.set(firstNonBlank(params.get("token"), params.get("loginToken"), params.get("login_token"), params.get("code")));
        byte[] body = "<html><body><h3>Login completed. You can close this window.</h3></body></html>"
            .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html;charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
        latch.countDown();
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> map = new HashMap<>();
        if (isBlank(query)) {
            return map;
        }
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            if (idx <= 0) {
                continue;
            }
            String key = decode(pair.substring(0, idx));
            String value = decode(pair.substring(idx + 1));
            map.put(key, value);
        }
        return map;
    }

    private String resolveSsoProfileUrl(String ssoBaseUrl, String ssoAppId) {
        String path = valueOrDefault(config.get("ssoProfilePath"), "/api/sso/v1/me/profile");
        String url = ssoBaseUrl + path;
        if (!isBlank(ssoAppId)) {
            url = url + "?appid=" + urlEncode(ssoAppId);
        }
        return url;
    }

    private String buildBrowserLoginUrl(String ssoBaseUrl, String ssoAppId, String callbackUrl) {
        String url = ssoBaseUrl + "/login?appid=" + urlEncode(ssoAppId);
        String mode = valueOrDefault(config.get("ssoBrowserLoginMode"), "login_token").toLowerCase();
        boolean appendRedirect = "redirect_uri".equals(mode)
            || "redirect-uri".equals(mode)
            || "redirecturi".equals(mode)
            || "explicit_redirect".equals(mode);
        if (appendRedirect && !isBlank(callbackUrl)) {
            url = url + "&redirectUri=" + urlEncode(callbackUrl)
                + "&redirect_uri=" + urlEncode(callbackUrl);
        }
        return url;
    }

    private String resolveAppBaseUrl() {
        String base = firstNonBlank(config.get("appBaseUrl"), config.get("baseUrl"));
        if (!isBlank(base)) {
            return trimTrailingSlash(base);
        }
        String apiBase = trimTrailingSlash(config.get("serverClientApiBase"));
        int idx = apiBase.indexOf("/api/");
        if (idx > 0) {
            return apiBase.substring(0, idx);
        }
        return "https://hisplus.zhunyintech.com";
    }

    private String resolvePhisBaseUrl() {
        return trimTrailingSlash(firstNonBlank(config.get("phisBaseUrl"), resolveAppBaseUrl()));
    }

    private void notify(Consumer<String> callback, String message) {
        if (callback != null) {
            callback.accept(message);
        }
        logger.info(message);
    }

    private String decode(String value) {
        return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String trimTrailingSlash(String value) {
        if (isBlank(value)) {
            return "";
        }
        String v = value.trim();
        return v.endsWith("/") ? v.substring(0, v.length() - 1) : v;
    }

    private String valueOrDefault(String value, String defaultValue) {
        return isBlank(value) ? defaultValue : value.trim();
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

    private String maskToken(String tokenValue) {
        if (isBlank(tokenValue)) {
            return null;
        }
        String token = tokenValue.trim();
        if (token.length() <= 10) {
            return "***(" + token.length() + ")";
        }
        return token.substring(0, 6) + "..." + token.substring(token.length() - 4);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
