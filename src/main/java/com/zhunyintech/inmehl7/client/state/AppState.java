package com.zhunyintech.inmehl7.client.state;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.db.SQLiteStore;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.LicenseSnapshot;

import java.awt.Desktop;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class AppState {

    private final Config config;
    private final SQLiteStore store;
    private final RuntimeLogger logger;

    private volatile String token;
    private volatile LicenseSnapshot licenseSnapshot = new LicenseSnapshot();

    public AppState(Config config, SQLiteStore store, RuntimeLogger logger) {
        this.config = config;
        this.store = store;
        this.logger = logger;
        this.token = store.loadAuthToken();
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
        if (token != null && !token.trim().isEmpty()) {
            store.saveAuthToken(token.trim());
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

    public boolean loginInBrowser(Consumer<String> statusCallback) {
        String ssoBaseUrl = trimTrailingSlash(config.get("ssoBaseUrl"));
        String ssoAppId = valueOrDefault(config.get("ssoAppId"), "inme-hl7");
        int callbackPort = config.getInt("ssoCallbackPort", 17689);
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
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().browse(URI.create(loginUrl));
            }

            boolean ok = latch.await(timeoutSec, TimeUnit.SECONDS);
            if (!ok) {
                notify(statusCallback, "SSO 登录回调超时");
                return false;
            }
            String receivedToken = tokenRef.get();
            if (receivedToken == null || receivedToken.trim().isEmpty()) {
                notify(statusCallback, "未获取到 token");
                return false;
            }
            setToken(receivedToken.trim());
            notify(statusCallback, "SSO 登录成功");
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

    private void handleCallback(HttpExchange exchange, AtomicReference<String> tokenRef, CountDownLatch latch) throws IOException {
        String query = exchange.getRequestURI() == null ? null : exchange.getRequestURI().getRawQuery();
        Map<String, String> params = parseQuery(query);
        String tokenValue = firstNonBlank(params.get("token"), params.get("loginToken"), params.get("login_token"), params.get("code"));
        tokenRef.set(tokenValue);
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
        if (query == null || query.trim().isEmpty()) {
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

    private String decode(String value) {
        return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    private void notify(Consumer<String> callback, String message) {
        if (callback != null) {
            callback.accept(message);
        }
        logger.info(message);
    }

    private String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String trimTrailingSlash(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "";
        }
        String v = value.trim();
        return v.endsWith("/") ? v.substring(0, v.length() - 1) : v;
    }

    private String valueOrDefault(String value, String defaultValue) {
        return (value == null || value.trim().isEmpty()) ? defaultValue : value.trim();
    }
}

