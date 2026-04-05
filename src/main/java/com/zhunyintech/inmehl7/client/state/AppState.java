package com.zhunyintech.inmehl7.client.state;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.db.ClientRepository;
import com.zhunyintech.inmehl7.client.db.SQLiteStore;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.LicenseSnapshot;
import com.zhunyintech.inmehl7.client.model.OrgInfo;

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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class AppState {

    private final Config config;
    private final SQLiteStore store;
    private final ClientRepository repository;
    private final RuntimeLogger logger;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final AtomicBoolean loginInProgress = new AtomicBoolean(false);

    private volatile String token;
    private volatile LicenseSnapshot licenseSnapshot = new LicenseSnapshot();
    private volatile OrgInfo currentOrgInfo;

    public AppState(Config config, SQLiteStore store, ClientRepository repository, RuntimeLogger logger) {
        this.config = config;
        this.store = store;
        this.repository = repository;
        this.logger = logger;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .version(HttpClient.Version.HTTP_1_1)
            .build();
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
        if (isBlank(this.token)) {
            store.clearAuthToken();
            return;
        }
        store.saveAuthToken(this.token);
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

    public boolean hasToken() {
        return !isBlank(token);
    }

    public boolean hasCompletedLogin() {
        return hasToken() && isValidOrgCode(currentOrgInfo == null ? null : currentOrgInfo.getOrgCode());
    }

    public boolean isLoginInProgress() {
        return loginInProgress.get();
    }

    public void clearSession() {
        setToken(null);
        currentOrgInfo = null;
        repository.clearOrgInfo();
        config.set("orgCode", "DEFAULT");
    }

    public boolean refreshCurrentOrg(Consumer<String> statusCallback) {
        if (isBlank(token)) {
            notify(statusCallback, "未登录，暂无法刷新机构信息");
            return false;
        }
        try {
            initializeCurrentOrg(statusCallback);
            return hasCompletedLogin();
        } catch (Exception ex) {
            logger.error("refreshCurrentOrg failed", ex);
            notify(statusCallback, "刷新机构信息失败: " + ex.getMessage());
            return false;
        }
    }

    public boolean loginInBrowser(Consumer<String> statusCallback) {
        if (!loginInProgress.compareAndSet(false, true)) {
            notify(statusCallback, "登录正在进行中，请勿重复点击");
            return false;
        }
        String ssoBaseUrl = trimTrailingSlash(config.get("ssoBaseUrl"));
        String ssoAppId = valueOrDefault(config.get("ssoAppId"), "inme-mid-monitor-client");
        String mode = valueOrDefault(config.get("ssoBrowserLoginMode"), "device_code").trim().toLowerCase();
        int timeoutSec = config.getInt("ssoCallbackTimeoutSec", 180);
        boolean tokenUpdated = false;

        try {
            String receivedToken;
            if (isDeviceCodeMode(mode)) {
                receivedToken = loginViaSsoDeviceCode(ssoBaseUrl, ssoAppId, timeoutSec, statusCallback);
            } else {
                receivedToken = loginViaBrowserCallback(ssoBaseUrl, ssoAppId, statusCallback);
            }
            if (isBlank(receivedToken)) {
                notify(statusCallback, "未获取到 token");
                return false;
            }
            setToken(receivedToken);
            tokenUpdated = true;
            notify(statusCallback, "SSO 登录成功，开始初始化机构信息");
            try {
                initializeCurrentOrg(statusCallback);
            } catch (Exception ex) {
                logger.error("initializeCurrentOrg failed", ex);
                notify(statusCallback, "机构信息初始化失败，但 token 已保存: " + ex.getMessage());
            }
            if (!hasCompletedLogin()) {
                clearSession();
                notify(statusCallback, "机构信息初始化失败，请重新登录");
                return false;
            }
            return true;
        } catch (Exception ex) {
            logger.error("loginInBrowser failed", ex);
            if (tokenUpdated) {
                clearSession();
            }
            notify(statusCallback, "SSO 登录失败: " + ex.getMessage());
            return false;
        } finally {
            loginInProgress.set(false);
        }
    }

    private String loginViaBrowserCallback(String ssoBaseUrl,
                                           String ssoAppId,
                                           Consumer<String> statusCallback) throws Exception {
        int callbackPort = config.getInt("ssoCallbackPort", 17654);
        int timeoutSec = config.getInt("ssoCallbackTimeoutSec", 180);
        String callbackUrl = "http://127.0.0.1:" + callbackPort + "/";
        String loginUrl = ssoBaseUrl + "/login?appid=" + urlEncode(ssoAppId)
            + "&redirectUri=" + urlEncode(callbackUrl)
            + "&redirect_uri=" + urlEncode(callbackUrl);

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
            openSystemBrowser(loginUrl);

            boolean ok = latch.await(timeoutSec, TimeUnit.SECONDS);
            if (!ok) {
                notify(statusCallback, "SSO 登录回调超时");
                return null;
            }
            return tokenRef.get();
        } finally {
            if (server != null) {
                try {
                    server.stop(0);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private String loginViaSsoDeviceCode(String ssoBaseUrl,
                                         String appId,
                                         int timeoutSec,
                                         Consumer<String> statusCallback) throws Exception {
        String deviceAuthUrl = ssoBaseUrl + "/oauth2/device_authorization";
        String tokenUrl = ssoBaseUrl + "/oauth2/token";
        String scope = valueOrDefault(config.get("ssoScope"), "openid profile offline_access");
        String clientSecret = blankToNull(config.get("ssoClientSecret"));
        List<String> authMethods = resolveClientAuthMethods(config.get("ssoClientAuthMethod"), clientSecret);

        JsonNode authJson = null;
        String authBody = null;
        int authStatus = 0;
        String usedAuthMethod = authMethods.isEmpty() ? "none" : authMethods.get(0);
        for (String authMethod : authMethods) {
            usedAuthMethod = authMethod;
            Map<String, String> form = new LinkedHashMap<>();
            form.put("client_id", appId);
            if (!isBlank(scope)) {
                form.put("scope", scope.trim());
            }
            HttpResponse<String> response = postForm(deviceAuthUrl, form, appId, clientSecret, authMethod);
            authStatus = response.statusCode();
            authBody = response.body();
            if (authStatus == 200) {
                authJson = mapper.readTree(authBody == null ? "" : authBody);
                break;
            }
            if (authStatus != 401) {
                break;
            }
        }

        if (authJson == null) {
            throw new IOException("获取设备验证码失败: HTTP " + authStatus + " " + safeOAuthMessage(authBody));
        }

        String deviceCode = text(authJson, "device_code");
        String userCode = text(authJson, "user_code");
        String verificationUri = text(authJson, "verification_uri");
        String verificationUriComplete = text(authJson, "verification_uri_complete");
        int expiresIn = authJson.path("expires_in").asInt(300);
        int interval = Math.max(2, authJson.path("interval").asInt(5));

        if (isBlank(deviceCode) || isBlank(verificationUri)) {
            throw new IOException("设备授权响应缺少必要字段");
        }

        String openUrl = firstNonBlank(verificationUriComplete, verificationUri);
        StringBuilder tip = new StringBuilder();
        tip.append("请在浏览器完成 Device Code Flow 登录/授权");
        if (!isBlank(userCode)) {
            tip.append(System.lineSeparator()).append("验证码: ").append(userCode);
        }
        tip.append(System.lineSeparator()).append("地址: ").append(verificationUri);
        notify(statusCallback, tip.toString());
        openSystemBrowser(openUrl);

        long now = System.currentTimeMillis();
        long deadline = now + Math.max(60, timeoutSec > 0 ? timeoutSec : expiresIn) * 1000L;
        deadline = Math.min(deadline, now + Math.max(60, expiresIn) * 1000L);
        notify(statusCallback, "等待浏览器完成登录/授权...");
        return pollDeviceAccessToken(tokenUrl, appId, deviceCode, interval, deadline, clientSecret, usedAuthMethod, authMethods, statusCallback);
    }

    private String pollDeviceAccessToken(String tokenUrl,
                                         String appId,
                                         String deviceCode,
                                         int intervalSec,
                                         long deadlineMs,
                                         String clientSecret,
                                         String preferredAuthMethod,
                                         List<String> configuredAuthMethods,
                                         Consumer<String> statusCallback) throws Exception {
        List<String> methods = buildTokenAuthMethods(configuredAuthMethods, preferredAuthMethod);
        int waitSec = Math.max(2, intervalSec);
        while (System.currentTimeMillis() < deadlineMs) {
            for (int attempt = 0; attempt < methods.size(); attempt++) {
                String authMethod = methods.get(attempt);
                Map<String, String> form = new LinkedHashMap<>();
                form.put("grant_type", "urn:ietf:params:oauth:grant-type:device_code");
                form.put("client_id", appId);
                form.put("device_code", deviceCode);

                HttpResponse<String> response = postForm(tokenUrl, form, appId, clientSecret, authMethod);
                int status = response.statusCode();
                String body = response.body();

                if (status == 200) {
                    JsonNode json = mapper.readTree(body == null ? "" : body);
                    String accessToken = text(json, "access_token");
                    if (!isBlank(accessToken)) {
                        return accessToken.trim();
                    }
                    throw new IOException("Token 响应缺少 access_token");
                }

                String error = oauthField(body, "error");
                String errorDescription = oauthField(body, "error_description");

                if ("invalid_client".equalsIgnoreCase(error) || status == 401) {
                    if (attempt < methods.size() - 1) {
                        continue;
                    }
                    throw new IOException("设备码换取 Token 失败: " + firstNonBlank(errorDescription, error, "invalid_client"));
                }
                if ("authorization_pending".equalsIgnoreCase(error)) {
                    break;
                }
                if ("slow_down".equalsIgnoreCase(error)) {
                    waitSec += 5;
                    break;
                }
                if ("expired_token".equalsIgnoreCase(error)) {
                    throw new IOException("Device Code 已过期，请重新登录");
                }
                if ("access_denied".equalsIgnoreCase(error)) {
                    throw new IOException("浏览器端已拒绝授权");
                }
                if (!isBlank(error) || status >= 400) {
                    throw new IOException("设备码换取 Token 失败: HTTP " + status + " " + firstNonBlank(errorDescription, error, safeOAuthMessage(body)));
                }
            }
            TimeUnit.SECONDS.sleep(waitSec);
            notify(statusCallback, "仍在等待浏览器授权完成...");
        }
        throw new IOException("浏览器授权超时");
    }

    private HttpResponse<String> postForm(String url,
                                          Map<String, String> form,
                                          String clientId,
                                          String clientSecret,
                                          String authMethod) throws IOException, InterruptedException {
        Map<String, String> finalForm = new LinkedHashMap<>();
        if (form != null) {
            finalForm.putAll(form);
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(15))
            .header("accept", "application/json")
            .header("Content-Type", "application/x-www-form-urlencoded");

        String mode = valueOrDefault(authMethod, "none").trim().toLowerCase();
        if ("basic".equals(mode) || "client_secret_basic".equals(mode) || "client-secret-basic".equals(mode)) {
            String raw = clientId + ":" + (clientSecret == null ? "" : clientSecret);
            builder.header("Authorization", "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8)));
        } else if ("post".equals(mode) || "client_secret_post".equals(mode) || "client-secret-post".equals(mode)) {
            finalForm.put("client_secret", clientSecret == null ? "" : clientSecret);
        }

        builder.POST(HttpRequest.BodyPublishers.ofString(buildFormBody(finalForm), StandardCharsets.UTF_8));
        logger.info("HTTP POST " + url);
        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        logger.info("HTTP POST " + url + " -> " + response.statusCode());
        return response;
    }

    private String buildFormBody(Map<String, String> form) {
        if (form == null || form.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : form.entrySet()) {
            if (!first) {
                sb.append('&');
            }
            first = false;
            sb.append(urlEncode(entry.getKey())).append('=').append(urlEncode(entry.getValue() == null ? "" : entry.getValue()));
        }
        return sb.toString();
    }

    private List<String> resolveClientAuthMethods(String configuredMethod, String clientSecret) {
        String mode = valueOrDefault(configuredMethod, "auto").trim().toLowerCase();
        if ("auto".equals(mode)) {
            if (!isBlank(clientSecret)) {
                List<String> methods = new ArrayList<>();
                methods.add("basic");
                methods.add("post");
                return methods;
            }
            return Collections.singletonList("none");
        }
        if ("basic".equals(mode) || "client_secret_basic".equals(mode) || "client-secret-basic".equals(mode)) {
            return Collections.singletonList("basic");
        }
        if ("post".equals(mode) || "client_secret_post".equals(mode) || "client-secret-post".equals(mode)) {
            return Collections.singletonList("post");
        }
        if ("none".equals(mode) || "public".equals(mode)) {
            return Collections.singletonList("none");
        }
        return Collections.singletonList(mode);
    }

    private List<String> buildTokenAuthMethods(List<String> configuredAuthMethods, String preferredMethod) {
        List<String> methods = new ArrayList<>();
        if (!isBlank(preferredMethod)) {
            methods.add(preferredMethod.trim());
        }
        if (configuredAuthMethods != null) {
            for (String method : configuredAuthMethods) {
                if (isBlank(method)) {
                    continue;
                }
                if (!methods.contains(method.trim())) {
                    methods.add(method.trim());
                }
            }
        }
        if (methods.isEmpty()) {
            methods.add("none");
        }
        return methods;
    }

    private boolean isDeviceCodeMode(String mode) {
        if (isBlank(mode)) {
            return true;
        }
        String normalized = mode.trim().toLowerCase();
        return "device_code".equals(normalized)
            || "device-code".equals(normalized)
            || "devicecode".equals(normalized)
            || "device".equals(normalized)
            || "auto".equals(normalized);
    }

    private void initializeCurrentOrg(Consumer<String> statusCallback) throws IOException, InterruptedException {
        String ssoBaseUrl = trimTrailingSlash(config.get("ssoBaseUrl"));
        String ssoAppId = valueOrDefault(config.get("ssoAppId"), "inme-mid-monitor-client");

        JsonNode ssoProfile = getJsonSafe(resolveSsoProfileUrl(ssoBaseUrl, ssoAppId), "SSO 个人信息", statusCallback);
        JsonNode authMe = getJsonSafe(resolveAppBaseUrl() + valueOrDefault(config.get("appAuthMePath"), "/api/zyhisplus/v1/auth/me"), "业务端个人信息", statusCallback);
        JsonNode authOrgs = getJsonSafe(resolveAppBaseUrl() + valueOrDefault(config.get("appAuthOrgsPath"), "/api/zyhisplus/v1/auth/orgs"), "业务端机构列表", statusCallback);

        String orgCode = firstNonBlank(
            normalizeOrgCode(text(authMe, "orgCode", "orgcode")),
            normalizeOrgCode(text(ssoProfile, "orgCode", "orgcode")),
            normalizeOrgCode(text(firstArrayObject(authOrgs), "orgCode", "orgcode"))
        );
        if (isBlank(orgCode)) {
            orgCode = normalizeConfiguredOrgCode(config.get("orgCode"));
        }
        JsonNode orgNode = null;
        if (!isBlank(orgCode)) {
            String orgInfoPath = valueOrDefault(config.get("orgInfoPath"), "/api/zyhisplus/v1/phisstockinvi/getPaykindInfo/{orgCode}");
            String url = resolvePhisBaseUrl() + orgInfoPath.replace("{orgCode}", urlEncode(orgCode));
            orgNode = getJsonSafe(url, "机构详情", statusCallback);
        }

        OrgInfo orgInfo = mergeOrgInfo(orgCode, ssoProfile, authMe, firstArrayObject(authOrgs), orgNode);
        if (orgInfo == null || isBlank(orgInfo.getOrgCode())) {
            throw new IOException("未解析到机构编码");
        }
        if (orgInfo == null || isBlank(orgInfo.getOrgCode())) {
            throw new IOException("未解析到机构编码，请确认 SSO 机构信息或业务端授权接口配置");
        }
        repository.saveOrgInfo(orgInfo);
        currentOrgInfo = orgInfo;
        config.set("orgCode", orgInfo.getOrgCode());
        notify(statusCallback, "机构初始化完成: " + orgInfo.getOrgCode() + " / " + firstNonBlank(orgInfo.getOrgName(), "-"));
    }

    private OrgInfo mergeOrgInfo(String orgCode, JsonNode ssoProfile, JsonNode authMe, JsonNode authOrg, JsonNode orgNode) {
        String resolvedOrgCode = normalizeOrgCode(firstNonBlank(
            orgCode,
            text(authMe, "orgCode", "orgcode"),
            text(ssoProfile, "orgCode", "orgcode"),
            text(authOrg, "orgCode", "orgcode")
        ));
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
        logger.info("HTTP GET " + url);
        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        logger.info("HTTP GET " + url + " -> " + response.statusCode());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("GET " + url + " -> HTTP " + response.statusCode() + " " + response.body());
        }
        return unwrap(response.body(), "GET " + url);
    }

    private JsonNode unwrap(String responseBody, String context) throws IOException {
        if (isBlank(responseBody)) {
            return mapper.nullNode();
        }
        JsonNode node = mapper.readTree(responseBody);
        JsonNode codeNode = node.get("code");
        if (codeNode != null && codeNode.isInt() && codeNode.asInt(0) != 0) {
            throw new IOException(firstNonBlank(text(node, "message", "msg"), "接口返回失败"));
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
        String tokenValue = token.trim();
        String headerName = valueOrDefault(config.get("authHeaderName"), "x-auth-token");
        String mode = resolveAuthHeaderMode(tokenValue);
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
        String mode = valueOrDefault(config.get("authHeaderMode"), "auto").trim().toLowerCase();
        if (isBlank(mode) || "auto".equals(mode)) {
            return looksLikeJwt(tokenValue) ? "authorization" : "x-auth-token";
        }
        if ("x_auth_token".equals(mode) || "xauthtoken".equals(mode)) {
            return "x-auth-token";
        }
        return mode;
    }

    private boolean looksLikeJwt(String tokenValue) {
        if (isBlank(tokenValue)) {
            return false;
        }
        String value = tokenValue.trim();
        int firstDot = value.indexOf('.');
        if (firstDot <= 0) {
            return false;
        }
        return value.indexOf('.', firstDot + 1) > firstDot + 1;
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
            if (isBlank(field)) {
                continue;
            }
            JsonNode valueNode = node.get(field);
            if (valueNode != null && !valueNode.isNull()) {
                String value = valueNode.asText();
                if (!isBlank(value)) {
                    return value.trim();
                }
            }
        }
        return null;
    }

    private String oauthField(String body, String field) {
        if (isBlank(body) || isBlank(field)) {
            return null;
        }
        try {
            JsonNode node = mapper.readTree(body);
            JsonNode valueNode = node.get(field);
            if (valueNode == null || valueNode.isNull()) {
                return null;
            }
            String value = valueNode.asText();
            return isBlank(value) ? null : value.trim();
        } catch (Exception ignored) {
            return null;
        }
    }

    private String safeOAuthMessage(String body) {
        String error = oauthField(body, "error");
        String errorDescription = oauthField(body, "error_description");
        String message = oauthField(body, "message");
        return firstNonBlank(errorDescription, message, error, body);
    }

    private void handleCallback(HttpExchange exchange, AtomicReference<String> tokenRef, CountDownLatch latch) throws IOException {
        String query = exchange.getRequestURI() == null ? null : exchange.getRequestURI().getRawQuery();
        Map<String, String> params = parseQuery(query);
        tokenRef.set(firstNonBlank(params.get("token"), params.get("loginToken"), params.get("login_token"), params.get("code")));
        byte[] body = "<html><body><h3>登录已完成，可以关闭此页面。</h3></body></html>"
            .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html;charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
        latch.countDown();
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> map = new LinkedHashMap<>();
        if (isBlank(query)) {
            return map;
        }
        for (String pair : query.split("&")) {
            int idx = pair.indexOf('=');
            if (idx <= 0) {
                continue;
            }
            map.put(decode(pair.substring(0, idx)), decode(pair.substring(idx + 1)));
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

    private String normalizeConfiguredOrgCode(String value) {
        String normalized = normalizeOrgCode(value);
        if (isBlank(normalized) || "DEFAULT".equalsIgnoreCase(normalized)) {
            return null;
        }
        return normalized;
    }

    private void notify(Consumer<String> callback, String message) {
        String localized = localizeStatusMessage(message);
        if (callback != null) {
            callback.accept(localized);
        }
        logger.info(localized);
    }

    private String localizeStatusMessage(String message) {
        if (isBlank(message)) {
            return message;
        }
        String text = message;
        text = text.replace("鏈櫥褰曪紝鏆傛棤娉曞埛鏂版満鏋勪俊鎭", "未登录，暂无法刷新机构信息");
        text = text.replace("鍒锋柊鏈烘瀯淇℃伅澶辫触", "刷新机构信息失败");
        text = text.replace("鐧诲綍姝ｅ湪杩涜涓紝璇峰嬁閲嶅鐐瑰嚮", "登录正在进行中，请勿重复点击");
        text = text.replace("鏈幏鍙栧埌 token", "未获取到 token");
        text = text.replace("SSO 鐧诲綍鎴愬姛锛屽紑濮嬪垵濮嬪寲鏈烘瀯淇℃伅", "SSO 登录成功，开始初始化机构信息");
        text = text.replace("鏈烘瀯淇℃伅鍒濆鍖栧け璐ワ紝浣?token 宸蹭繚瀛?", "机构信息初始化失败，token 已保存");
        text = text.replace("鏈烘瀯淇℃伅鍒濆鍖栧け璐ワ紝璇烽噸鏂扮櫥褰", "机构信息初始化失败，请重新登录");
        text = text.replace("SSO 鐧诲綍澶辫触", "SSO 登录失败");
        text = text.replace("璇峰湪娴忚鍣ㄥ畬鎴?SSO 鐧诲綍", "请在浏览器完成 SSO 登录");
        text = text.replace("SSO 鐧诲綍鍥炶皟瓒呮椂", "SSO 登录回调超时");
        text = text.replace("鑾峰彇璁惧楠岃瘉鐮佸け璐", "获取设备验证码失败");
        text = text.replace("璁惧鎺堟潈鍝嶅簲缂哄皯蹇呰瀛楁", "设备授权响应缺少必要字段");
        text = text.replace("璇峰湪娴忚鍣ㄥ畬鎴?Device Code Flow 鐧诲綍/鎺堟潈", "请在浏览器完成 Device Code Flow 登录/授权");
        text = text.replace("楠岃瘉鐮?", "验证码:");
        text = text.replace("鍦板潃:", "地址:");
        text = text.replace("绛夊緟娴忚鍣ㄥ畬鎴愮櫥褰?鎺堟潈...", "等待浏览器完成登录/授权...");
        text = text.replace("Token 鍝嶅簲缂哄皯 access_token", "Token 响应缺少 access_token");
        text = text.replace("璁惧鐮佹崲鍙?Token 澶辫触", "设备码换取 Token 失败");
        text = text.replace("Device Code 宸茶繃鏈燂紝璇烽噸鏂扮櫥褰", "Device Code 已过期，请重新登录");
        text = text.replace("娴忚鍣ㄧ宸叉嫆缁濇巿鏉", "浏览器端已拒绝授权");
        text = text.replace("浠嶅湪绛夊緟娴忚鍣ㄦ巿鏉冨畬鎴?\\..", "仍在等待浏览器授权完成...");
        text = text.replace("浠嶅湪绛夊緟娴忚鍣ㄦ巿鏉冨畬鎴?..", "仍在等待浏览器授权完成...");
        text = text.replace("娴忚鍣ㄦ巿鏉冭秴鏃", "浏览器授权超时");
        text = text.replace("SSO 涓汉淇℃伅", "SSO 个人信息");
        text = text.replace("涓氬姟绔釜浜轰俊鎭", "业务端个人信息");
        text = text.replace("涓氬姟绔満鏋勫垪琛", "业务端机构列表");
        text = text.replace("鏈烘瀯璇︽儏", "机构详情");
        text = text.replace("鏈В鏋愬埌鏈烘瀯缂栫爜", "未解析到机构编码");
        text = text.replace("鏈烘瀯鍒濆鍖栧畬鎴?", "机构初始化完成:");
        text = text.replace("鎺ュ彛杩斿洖澶辫触", "接口返回失败");
        text = text.replace("鑾峰彇澶辫触", "获取失败");
        text = text.replace("Login completed. You can close this window.", "登录已完成，可以关闭此页面。");
        return text;
    }

    private void openSystemBrowser(String url) {
        if (isBlank(url)) {
            return;
        }
        try {
            logger.info("OPEN BROWSER " + url);
            if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                Desktop.getDesktop().browse(URI.create(url));
            }
        } catch (Exception ex) {
            logger.warn("打开浏览器失败: " + ex.getMessage());
        }
    }

    private String decode(String value) {
        return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private String urlEncode(String value) {
        return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
    }

    private String trimTrailingSlash(String value) {
        if (isBlank(value)) {
            return "";
        }
        String trimmed = value.trim();
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }

    private String valueOrDefault(String value, String defaultValue) {
        return isBlank(value) ? defaultValue : value.trim();
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

    private boolean isValidOrgCode(String value) {
        return !isBlank(normalizeOrgCode(value));
    }

    private String normalizeOrgCode(String value) {
        if (isBlank(value)) {
            return null;
        }
        String normalized = value.trim();
        return "DEFAULT".equalsIgnoreCase(normalized) ? null : normalized;
    }

    private String maskToken(String tokenValue) {
        if (isBlank(tokenValue)) {
            return null;
        }
        String value = tokenValue.trim();
        if (value.length() <= 10) {
            return "***(" + value.length() + ")";
        }
        return value.substring(0, 6) + "..." + value.substring(value.length() - 4);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
