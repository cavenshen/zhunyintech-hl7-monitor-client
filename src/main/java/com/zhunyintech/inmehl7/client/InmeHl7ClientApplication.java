package com.zhunyintech.inmehl7.client;

import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.db.SQLiteStore;
import com.zhunyintech.inmehl7.client.hl7.Hl7MessageParser;
import com.zhunyintech.inmehl7.client.hl7.MllpServer;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.Hl7ResultRecord;
import com.zhunyintech.inmehl7.client.model.LicenseSnapshot;
import com.zhunyintech.inmehl7.client.state.AppState;
import com.zhunyintech.inmehl7.client.sync.ServerApiClient;
import com.zhunyintech.inmehl7.client.sync.SyncScheduler;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InmeHl7ClientApplication extends Application {

    private final ExecutorService ioExecutor = Executors.newCachedThreadPool();
    private final Hl7MessageParser parser = new Hl7MessageParser();

    private Config config;
    private RuntimeLogger logger;
    private SQLiteStore store;
    private AppState appState;
    private ServerApiClient apiClient;
    private SyncScheduler syncScheduler;
    private MllpServer mllpServer;

    private Label tokenLabel;
    private Label listenerLabel;
    private Label licenseLabel;
    private TextArea logArea;

    private TextField ssoBaseUrlField;
    private TextField ssoAppIdField;
    private TextField serverApiBaseField;
    private TextField clientCodeField;
    private TextField orgCodeField;
    private TextField deviceIdField;
    private TextField listenerPortField;
    private TextField syncIntervalField;
    private TextField heartbeatIntervalField;

    @Override
    public void start(Stage stage) {
        initRuntime();
        VBox root = new VBox(10);
        root.setPadding(new Insets(12));
        root.getChildren().add(buildStatusBox());
        root.getChildren().add(buildConfigBox());
        root.getChildren().add(buildActionBox());
        root.getChildren().add(buildLogBox());
        VBox.setVgrow(logArea, Priority.ALWAYS);

        stage.setTitle("inme-hl7 monitor client");
        stage.setScene(new Scene(root, 1100, 760));
        stage.show();

        appendLog("Client started");
        refreshStatusLabels();
        syncScheduler.start();
    }

    @Override
    public void stop() {
        if (syncScheduler != null) {
            syncScheduler.stop();
        }
        stopListener();
        ioExecutor.shutdownNow();
    }

    private void initRuntime() {
        this.config = new Config();
        this.logger = new RuntimeLogger(config);
        this.store = new SQLiteStore(config, logger);
        this.store.init();
        this.appState = new AppState(config, store, logger);
        this.apiClient = new ServerApiClient(config, logger);
        this.syncScheduler = new SyncScheduler(
            config, logger, store, appState, apiClient,
            () -> mllpServer != null && mllpServer.isRunning(),
            this::appendLog
        );
    }

    private VBox buildStatusBox() {
        tokenLabel = new Label();
        listenerLabel = new Label();
        licenseLabel = new Label();

        VBox box = new VBox(4);
        box.getChildren().addAll(tokenLabel, listenerLabel, licenseLabel);
        return box;
    }

    private GridPane buildConfigBox() {
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(8);

        int row = 0;
        ssoBaseUrlField = addField(grid, row++, "ssoBaseUrl", config.get("ssoBaseUrl"));
        ssoAppIdField = addField(grid, row++, "ssoAppId", config.get("ssoAppId"));
        serverApiBaseField = addField(grid, row++, "serverClientApiBase", config.get("serverClientApiBase"));
        clientCodeField = addField(grid, row++, "clientCode", config.get("clientCode"));
        orgCodeField = addField(grid, row++, "orgCode", config.get("orgCode"));
        deviceIdField = addField(grid, row++, "deviceId", config.get("deviceId"));
        listenerPortField = addField(grid, row++, "hl7ListenerPort", config.get("hl7ListenerPort"));
        syncIntervalField = addField(grid, row++, "syncIntervalSec", config.get("syncIntervalSec"));
        heartbeatIntervalField = addField(grid, row++, "heartbeatIntervalSec", config.get("heartbeatIntervalSec"));
        return grid;
    }

    private HBox buildActionBox() {
        Button saveBtn = new Button("Save Config");
        saveBtn.setOnAction(e -> saveConfig());

        Button loginBtn = new Button("SSO Login");
        loginBtn.setOnAction(e -> loginSso());

        Button startBtn = new Button("Start Listener");
        startBtn.setOnAction(e -> startListener());

        Button stopBtn = new Button("Stop Listener");
        stopBtn.setOnAction(e -> stopListener());

        Button syncBtn = new Button("Sync Now");
        syncBtn.setOnAction(e -> syncScheduler.triggerSyncNow());

        HBox box = new HBox(8, saveBtn, loginBtn, startBtn, stopBtn, syncBtn);
        return box;
    }

    private VBox buildLogBox() {
        logArea = new TextArea();
        logArea.setEditable(false);
        logArea.setWrapText(true);
        logger.bindSink(this::appendLog);
        return new VBox(logArea);
    }

    private TextField addField(GridPane grid, int row, String label, String value) {
        Label l = new Label(label);
        TextField field = new TextField(value == null ? "" : value);
        grid.add(l, 0, row);
        grid.add(field, 1, row);
        GridPane.setHgrow(field, Priority.ALWAYS);
        return field;
    }

    private void saveConfig() {
        config.set("ssoBaseUrl", ssoBaseUrlField.getText());
        config.set("ssoAppId", ssoAppIdField.getText());
        config.set("serverClientApiBase", serverApiBaseField.getText());
        config.set("clientCode", clientCodeField.getText());
        config.set("orgCode", orgCodeField.getText());
        config.set("deviceId", deviceIdField.getText());
        config.set("hl7ListenerPort", listenerPortField.getText());
        config.set("syncIntervalSec", syncIntervalField.getText());
        config.set("heartbeatIntervalSec", heartbeatIntervalField.getText());
        try {
            config.saveExternal();
            appendLog("Config saved");
            store.saveRuntimeEvent("CONFIG", "INFO", "ui", "Config saved", null);
        } catch (Exception ex) {
            appendLog("Config save failed: " + ex.getMessage());
            store.saveRuntimeEvent("CONFIG", "ERROR", "ui", ex.getMessage(), null);
        }
    }

    private void loginSso() {
        ioExecutor.submit(() -> {
            boolean ok = appState.loginInBrowser(this::appendLog);
            Platform.runLater(() -> {
                refreshStatusLabels();
                if (ok) {
                    store.saveRuntimeEvent("SSO", "INFO", "auth", "SSO login success", null);
                    syncScheduler.triggerSyncNow();
                } else {
                    store.saveRuntimeEvent("SSO", "ERROR", "auth", "SSO login failed", null);
                }
            });
        });
    }

    private void startListener() {
        if (mllpServer != null && mllpServer.isRunning()) {
            appendLog("Listener already running");
            return;
        }
        int port;
        try {
            port = Integer.parseInt(listenerPortField.getText().trim());
        } catch (Exception ex) {
            appendLog("Invalid listener port");
            return;
        }
        mllpServer = new MllpServer(port, logger, this::handleHl7Message);
        mllpServer.start();
        store.saveRuntimeEvent("MLLP", "INFO", "listener", "Listener started at " + port, null);
        refreshStatusLabels();
    }

    private void stopListener() {
        if (mllpServer != null) {
            mllpServer.stop();
            mllpServer = null;
            store.saveRuntimeEvent("MLLP", "INFO", "listener", "Listener stopped", null);
        }
        refreshStatusLabels();
    }

    private String handleHl7Message(String message) {
        Hl7MessageParser.ParseResult result = parser.parseOru(message, "INMEHL7CLIENT", config.get("orgCode"));
        if (result.getAckMessage() != null) {
            store.saveRawMessage(message, result.isAccepted(), result.getErrorMessage());
        } else {
            store.saveRawMessage(message, false, result.getErrorMessage());
        }
        if (result.isSuccess()) {
            Hl7ResultRecord record = result.getRecord();
            store.saveResult(config.get("clientCode"), config.get("orgCode"), config.get("deviceId"), record);
            appendLog("HL7 ORU received, sampleNo=" + record.getSampleNo() + ", items=" + record.getObservations().size());
            store.saveRuntimeEvent("HL7", "INFO", "listener", "ORU received " + record.getMessageControlId(), null);
        } else {
            appendLog("HL7 parse rejected: " + result.getErrorMessage());
            store.saveRuntimeEvent("HL7", "WARN", "listener", result.getErrorMessage(), message);
        }
        return result.getAckMessage();
    }

    private void refreshStatusLabels() {
        String token = appState.getToken();
        tokenLabel.setText("Token: " + (token == null || token.isEmpty() ? "Not logged in" : "Logged in"));
        listenerLabel.setText("Listener: " + ((mllpServer != null && mllpServer.isRunning()) ? "RUNNING" : "STOPPED"));
        LicenseSnapshot snapshot = appState.getLicenseSnapshot();
        String text = "License: " + safe(snapshot.getLicenseStatus()) + ", daysLeft=" + snapshot.getDaysLeft() + ", " + safe(snapshot.getMessage());
        licenseLabel.setText(text);
    }

    private String safe(String value) {
        return value == null ? "-" : value;
    }

    private void appendLog(String line) {
        if (line == null || line.trim().isEmpty()) {
            return;
        }
        Platform.runLater(() -> {
            logArea.appendText(line + System.lineSeparator());
            refreshStatusLabels();
        });
    }

    public static void main(String[] args) {
        launch(args);
    }
}

