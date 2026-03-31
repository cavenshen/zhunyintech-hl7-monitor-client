package com.zhunyintech.inmehl7.client;

import java.awt.Desktop;
import com.zhunyintech.inmehl7.client.config.Config;
import com.zhunyintech.inmehl7.client.db.ClientRepository;
import com.zhunyintech.inmehl7.client.db.SQLiteStore;
import com.zhunyintech.inmehl7.client.export.DataExportService;
import com.zhunyintech.inmehl7.client.hl7.Hl7MessageParser;
import com.zhunyintech.inmehl7.client.hl7.ListenerManager;
import com.zhunyintech.inmehl7.client.hl7.MllpServer;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.DeviceRegistry;
import com.zhunyintech.inmehl7.client.model.Hl7ResultRecord;
import com.zhunyintech.inmehl7.client.model.LicenseSnapshot;
import com.zhunyintech.inmehl7.client.model.MedicalDataItem;
import com.zhunyintech.inmehl7.client.model.MedicalDataRecord;
import com.zhunyintech.inmehl7.client.model.OrgInfo;
import com.zhunyintech.inmehl7.client.state.AppState;
import com.zhunyintech.inmehl7.client.sync.ServerApiClient;
import com.zhunyintech.inmehl7.client.sync.SyncScheduler;
import com.zhunyintech.inmehl7.client.ventilator.VentilatorRecordSupport;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.Alert;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.util.StringConverter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InmeHl7ClientApplication extends Application {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ExecutorService ioExecutor = Executors.newCachedThreadPool();
    private final Hl7MessageParser parser = new Hl7MessageParser();
    private final DataExportService exportService = new DataExportService();

    private Config config;
    private RuntimeLogger logger;
    private SQLiteStore store;
    private ClientRepository repository;
    private AppState appState;
    private ServerApiClient apiClient;
    private SyncScheduler syncScheduler;
    private ListenerManager listenerManager;
    private Stage primaryStage;
    private Scene loginScene;
    private Scene mainScene;
    private final AtomicBoolean uiLoginInProgress = new AtomicBoolean(false);

    private TextField loginSsoBaseUrlField;
    private TextField loginSsoAppIdField;
    private TextField loginAppBaseUrlField;
    private TextField loginServerApiBaseField;
    private Label loginStatusLabel;
    private TextArea loginLogArea;
    private Button loginButton;
    private Button loginSaveButton;

    private Label tokenLabel;
    private Label orgLabel;
    private Label listenerLabel;
    private Label licenseLabel;
    private Label summaryLabel;
    private TextArea logArea;

    private TextField ssoBaseUrlField;
    private TextField ssoAppIdField;
    private TextField appBaseUrlField;
    private TextField serverApiBaseField;
    private TextField clientCodeField;
    private TextField orgCodeField;
    private TextField deviceIdField;
    private TextField listenerPortField;
    private TextField syncIntervalField;
    private TextField heartbeatIntervalField;
    private TextField exportDirField;

    private TableView<DeviceRegistry> deviceTable;
    private TextField deviceTypeField;
    private TextField registryDeviceIdField;
    private TextField macAddressField;
    private TextField sourceIpField;
    private TextField sourcePortField;
    private TextField deviceListenPortField;
    private TextField nicknameField;
    private TextField wardNameField;
    private TextField bedNameField;
    private TextField patientIdField;
    private TextField patientNameField;
    private TextField patientNoField;
    private TextField admissionNoField;
    private ComboBox<String> ackModeCombo;
    private CheckBox deviceEnabledCheck;

    private ComboBox<DeviceRegistry> recordDeviceFilter;
    private TextField patientKeywordField;
    private TextField recordLimitField;
    private TableView<MedicalDataRecord> recordTable;
    private TextArea detailArea;
    private TextArea rawArea;
    private List<MedicalDataRecord> currentRecords = new ArrayList<>();

    @Override
    public void start(Stage stage) {
        this.primaryStage = stage;
        initRuntime();
        stage.setTitle("inme-hl7 monitor client");
        if (appState.hasToken() || appState.hasCompletedLogin()) {
            appState.clearSession();
            appendLog("已清理本地登录状态，进入系统前请先完成浏览器 SSO 登录。");
        }
        showLoginScene("请先完成浏览器 SSO 登录，再进入客户端。");
        stage.show();
        appendLog("客户端已启动，等待浏览器 SSO 登录。");
        /*
        VBox root = new VBox(10);
        root.setPadding(new Insets(12));
        root.getChildren().add(buildStatusBox());
        root.getChildren().add(buildConfigBox());
        root.getChildren().add(buildActionBox());

        TabPane tabPane = new TabPane();
        tabPane.getTabs().add(new Tab("设备台账", buildDeviceTab()));
        tabPane.getTabs().add(new Tab("数据记录", buildRecordTab()));
        tabPane.getTabs().add(new Tab("运行日志", buildLogTab()));
        tabPane.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        VBox.setVgrow(tabPane, Priority.ALWAYS);
        root.getChildren().add(tabPane);

        stage.setScene(new Scene(root, 1380, 900));
        stage.show();

        appendLog("Client started");
        refreshStatusLabels();
        refreshDevicesAsync();
        queryRecordsAsync();
        syncScheduler.start();
        if (appState.getToken() != null && appState.getCurrentOrgInfo() == null) {
            ioExecutor.submit(() -> {
                appState.refreshCurrentOrg(this::appendLog);
                Platform.runLater(() -> {
                    orgCodeField.setText(appState.getCurrentOrgCode());
                    refreshStatusLabels();
                });
            });
        }
        */
    }

    @Override
    public void stop() {
        if (syncScheduler != null) {
            syncScheduler.stop();
        }
        stopListeners();
        ioExecutor.shutdownNow();
    }

    private void initRuntime() {
        config = new Config();
        logger = new RuntimeLogger(config);
        logger.bindSink(this::appendLog);
        store = new SQLiteStore(config, logger);
        repository = new ClientRepository(config, logger);
        store.init();
        repository.init();
        appState = new AppState(config, store, repository, logger);
        apiClient = new ServerApiClient(config, logger);
        listenerManager = new ListenerManager(logger, this::handleHl7Message);
        syncScheduler = new SyncScheduler(
            config, logger, store, appState, apiClient,
            () -> listenerManager != null && listenerManager.isRunning(),
            this::appendLog
        );
    }

    private void showLoginScene(String statusText) {
        if (syncScheduler != null) {
            syncScheduler.stop();
        }
        stopListeners();
        if (loginScene == null) {
            loginScene = buildLoginScene();
        }
        syncLoginConfigFields();
        if (loginStatusLabel != null) {
            loginStatusLabel.setText(valueOrDefault(statusText, "请使用浏览器完成 SSO 登录。"));
        }
        if (primaryStage != null) {
            primaryStage.setScene(loginScene);
            primaryStage.setWidth(860);
            primaryStage.setHeight(620);
            primaryStage.setMinWidth(860);
            primaryStage.setMinHeight(620);
        }
    }

    private Scene buildLoginScene() {
        VBox root = new VBox(12);
        root.setPadding(new Insets(20));

        Label title = new Label("HL7 客户端登录");
        title.setStyle("-fx-font-size: 22px; -fx-font-weight: bold;");

        Label hint = new Label("请先完成浏览器 SSO 登录。只有登录成功并完成机构初始化后，才能进入设备和数据页面。");
        hint.setWrapText(true);

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        int row = 0;
        loginSsoBaseUrlField = addField(grid, row++, "SSO 地址", config.get("ssoBaseUrl"));
        loginSsoAppIdField = addField(grid, row++, "应用 AppId", config.get("ssoAppId"));
        loginAppBaseUrlField = addField(grid, row++, "业务地址", config.get("appBaseUrl"));
        loginServerApiBaseField = addField(grid, row++, "客户端接口地址", config.get("serverClientApiBase"));

        loginSaveButton = new Button("保存配置");
        loginSaveButton.setOnAction(e -> saveLoginConfig());

        loginButton = new Button("浏览器 SSO 登录");
        loginButton.setDefaultButton(true);
        loginButton.setOnAction(e -> loginFromLoginScene());

        HBox actionBox = new HBox(8, loginSaveButton, loginButton);

        loginStatusLabel = new Label("请使用浏览器完成 SSO 登录。");
        loginStatusLabel.setWrapText(true);

        loginLogArea = new TextArea();
        loginLogArea.setEditable(false);
        loginLogArea.setWrapText(true);
        VBox.setVgrow(loginLogArea, Priority.ALWAYS);

        root.getChildren().addAll(title, hint, grid, actionBox, loginStatusLabel, loginLogArea);
        return new Scene(root, 860, 620);
    }

    private void syncLoginConfigFields() {
        if (loginSsoBaseUrlField != null) {
            loginSsoBaseUrlField.setText(valueOrDefault(config.get("ssoBaseUrl"), ""));
        }
        if (loginSsoAppIdField != null) {
            loginSsoAppIdField.setText(valueOrDefault(config.get("ssoAppId"), ""));
        }
        if (loginAppBaseUrlField != null) {
            loginAppBaseUrlField.setText(valueOrDefault(config.get("appBaseUrl"), ""));
        }
        if (loginServerApiBaseField != null) {
            loginServerApiBaseField.setText(valueOrDefault(config.get("serverClientApiBase"), ""));
        }
    }

    private void setLoginUiBusy(boolean busy) {
        uiLoginInProgress.set(busy);
        if (loginButton != null) {
            loginButton.setDisable(busy);
        }
        if (loginSaveButton != null) {
            loginSaveButton.setDisable(busy);
        }
        if (loginSsoBaseUrlField != null) {
            loginSsoBaseUrlField.setDisable(busy);
        }
        if (loginSsoAppIdField != null) {
            loginSsoAppIdField.setDisable(busy);
        }
        if (loginAppBaseUrlField != null) {
            loginAppBaseUrlField.setDisable(busy);
        }
        if (loginServerApiBaseField != null) {
            loginServerApiBaseField.setDisable(busy);
        }
    }

    private void saveLoginConfig() {
        config.set("ssoBaseUrl", loginSsoBaseUrlField == null ? config.get("ssoBaseUrl") : loginSsoBaseUrlField.getText());
        config.set("ssoAppId", loginSsoAppIdField == null ? config.get("ssoAppId") : loginSsoAppIdField.getText());
        config.set("appBaseUrl", loginAppBaseUrlField == null ? config.get("appBaseUrl") : loginAppBaseUrlField.getText());
        config.set("serverClientApiBase", loginServerApiBaseField == null ? config.get("serverClientApiBase") : loginServerApiBaseField.getText());
        try {
            config.saveExternal();
            syncLoginConfigFields();
            appendLog("登录配置已保存。");
            if (loginStatusLabel != null) {
                loginStatusLabel.setText("配置已保存，现在可以开始浏览器登录。");
            }
        } catch (Exception ex) {
            appendLog("保存登录配置失败: " + ex.getMessage());
            if (loginStatusLabel != null) {
                loginStatusLabel.setText("保存配置失败: " + ex.getMessage());
            }
        }
    }

    private void loginFromLoginScene() {
        if (uiLoginInProgress.get()) {
            return;
        }
        saveLoginConfig();
        setLoginUiBusy(true);
        if (loginStatusLabel != null) {
            loginStatusLabel.setText("正在打开浏览器，请在浏览器中完成 SSO 授权。");
        }
        ioExecutor.submit(() -> {
            boolean ok = appState.loginInBrowser(this::appendLog);
            Platform.runLater(() -> {
                setLoginUiBusy(false);
                if (!ok || !appState.hasCompletedLogin()) {
                    if (loginStatusLabel != null) {
                        loginStatusLabel.setText("登录尚未完成，请在浏览器授权完成后重试。");
                    }
                    return;
                }
                showMainSceneCn();
            });
        });
    }

    private void showMainSceneLegacy() {
        VBox root = new VBox(10);
        root.setPadding(new Insets(12));
        root.getChildren().add(buildStatusBox());
        root.getChildren().add(buildConfigBox());
        root.getChildren().add(buildActionBox());

        TabPane tabPane = new TabPane();
        tabPane.getTabs().add(new Tab("璁惧鍙拌处", buildDeviceTab()));
        tabPane.getTabs().add(new Tab("鏁版嵁璁板綍", buildRecordTab()));
        tabPane.getTabs().add(new Tab("杩愯鏃ュ織", buildLogTab()));
        tabPane.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        VBox.setVgrow(tabPane, Priority.ALWAYS);
        root.getChildren().add(tabPane);

        mainScene = new Scene(root, 1380, 900);
        if (primaryStage != null) {
            primaryStage.setScene(mainScene);
            primaryStage.setWidth(1380);
            primaryStage.setHeight(900);
            primaryStage.setMinWidth(1180);
            primaryStage.setMinHeight(760);
        }

        refreshStatusLabels();
        refreshDevicesAsync();
        queryRecordsAsync();
        syncScheduler.start();
        if (orgCodeField != null) {
            orgCodeField.setText(appState.getCurrentOrgCode());
        }
    }

    private void showMainScene() {
        VBox root = new VBox(10);
        root.setPadding(new Insets(12));
        root.getChildren().add(buildStatusBox());
        root.getChildren().add(buildConfigBox());
        root.getChildren().add(buildActionBox());

        TabPane tabPane = new TabPane();
        tabPane.getTabs().add(new Tab("设备台账", buildDeviceTab()));
        tabPane.getTabs().add(new Tab("数据记录", buildRecordTab()));
        tabPane.getTabs().add(new Tab("运行日志", buildLogTab()));
        tabPane.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        VBox.setVgrow(tabPane, Priority.ALWAYS);
        root.getChildren().add(tabPane);

        mainScene = new Scene(root, 1380, 900);
        if (primaryStage != null) {
            primaryStage.setScene(mainScene);
            primaryStage.setWidth(1380);
            primaryStage.setHeight(900);
            primaryStage.setMinWidth(1180);
            primaryStage.setMinHeight(760);
        }

        refreshStatusLabels();
        refreshDevicesAsync();
        queryRecordsAsync();
        syncScheduler.start();
        if (orgCodeField != null) {
            orgCodeField.setText(appState.getCurrentOrgCode());
        }
    }

    private void showMainSceneCn() {
        VBox root = new VBox(10);
        root.setPadding(new Insets(12));
        root.getChildren().add(buildStatusBox());
        root.getChildren().add(buildConfigBoxCn());
        root.getChildren().add(buildActionBoxCn());

        TabPane tabPane = new TabPane();
        tabPane.getTabs().add(new Tab("\u8bbe\u5907\u53f0\u8d26", buildDeviceTabCn()));
        tabPane.getTabs().add(new Tab("\u6570\u636e\u8bb0\u5f55", buildRecordTabCn()));
        tabPane.getTabs().add(new Tab("\u8fd0\u884c\u65e5\u5fd7", buildLogTab()));
        tabPane.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        VBox.setVgrow(tabPane, Priority.ALWAYS);
        root.getChildren().add(tabPane);

        mainScene = new Scene(root, 1380, 900);
        if (primaryStage != null) {
            primaryStage.setScene(mainScene);
            primaryStage.setWidth(1380);
            primaryStage.setHeight(900);
            primaryStage.setMinWidth(1180);
            primaryStage.setMinHeight(760);
        }

        refreshStatusLabels();
        refreshDevicesAsync();
        queryRecordsAsync();
        syncScheduler.start();
        if (orgCodeField != null) {
            orgCodeField.setText(appState.getCurrentOrgCode());
        }
    }

    private GridPane buildConfigBoxCn() {
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(8);
        int row = 0;
        ssoBaseUrlField = addField(grid, row++, "ssoBaseUrl", config.get("ssoBaseUrl"));
        ssoAppIdField = addField(grid, row++, "ssoAppId", config.get("ssoAppId"));
        appBaseUrlField = addField(grid, row++, "appBaseUrl", config.get("appBaseUrl"));
        serverApiBaseField = addField(grid, row++, "serverClientApiBase", config.get("serverClientApiBase"));
        clientCodeField = addField(grid, row++, "clientCode", config.get("clientCode"));
        orgCodeField = addField(grid, row++, "orgCode", appState.getCurrentOrgCode());
        deviceIdField = addField(grid, row++, "deviceId(\u517c\u5bb9)", config.get("deviceId"));
        listenerPortField = addField(grid, row++, "hl7ListenerPort", config.get("hl7ListenerPort"));
        syncIntervalField = addField(grid, row++, "syncIntervalSec", config.get("syncIntervalSec"));
        heartbeatIntervalField = addField(grid, row++, "heartbeatIntervalSec", config.get("heartbeatIntervalSec"));
        exportDirField = addField(grid, row, "exportDir", config.get("exportDir"));
        return grid;
    }

    private HBox buildActionBoxCn() {
        Button saveBtn = new Button("\u4fdd\u5b58\u914d\u7f6e");
        saveBtn.setOnAction(e -> saveConfig());

        Button loginBtn = new Button("\u91cd\u65b0\u767b\u5f55");
        loginBtn.setOnAction(e -> loginSso());

        Button refreshOrgBtn = new Button("\u5237\u65b0\u673a\u6784");
        refreshOrgBtn.setOnAction(e -> refreshOrg());

        Button startBtn = new Button("\u542f\u52a8\u76d1\u542c");
        startBtn.setOnAction(e -> startListeners());

        Button stopBtn = new Button("\u505c\u6b62\u76d1\u542c");
        stopBtn.setOnAction(e -> stopListeners());

        Button syncBtn = new Button("\u7acb\u5373\u540c\u6b65");
        syncBtn.setOnAction(e -> syncScheduler.triggerSyncNow());

        Button logoutBtn = new Button("\u9000\u51fa\u767b\u5f55");
        logoutBtn.setOnAction(e -> logoutToLoginScene());
        return new HBox(8, saveBtn, loginBtn, refreshOrgBtn, startBtn, stopBtn, syncBtn, logoutBtn);
    }

    private VBox buildDeviceTabCn() {
        deviceTable = new TableView<>();
        deviceTable.getColumns().add(column("\u8bbe\u5907ID", DeviceRegistry::getDeviceId));
        deviceTable.getColumns().add(column("\u6635\u79f0", DeviceRegistry::getDisplayName));
        deviceTable.getColumns().add(column("\u7c7b\u578b", DeviceRegistry::getDeviceType));
        deviceTable.getColumns().add(column("\u6765\u6e90IP", DeviceRegistry::getSourceIp));
        deviceTable.getColumns().add(column("\u76d1\u542c\u7aef\u53e3", d -> String.valueOf(d.getListenPort())));
        deviceTable.getColumns().add(column("\u75c5\u623f/\u75c5\u5e8a", d -> joinText(d.getWardName(), d.getBedName(), " / ")));
        deviceTable.getColumns().add(column("\u60a3\u8005", DeviceRegistry::getPatientName));
        deviceTable.getColumns().add(column("ACK", DeviceRegistry::getAckMode));
        deviceTable.getColumns().add(column("\u542f\u7528", d -> d.isEnabled() ? "\u662f" : "\u5426"));
        deviceTable.getColumns().add(column("\u6700\u8fd1\u6536\u5230", d -> text(d.getLastSeenAt())));
        deviceTable.getSelectionModel().selectedItemProperty().addListener((obs, old, value) -> fillDeviceForm(value));
        VBox.setVgrow(deviceTable, Priority.ALWAYS);

        GridPane form = new GridPane();
        form.setHgap(10);
        form.setVgap(8);
        int row = 0;
        deviceTypeField = addField(form, row++, "\u8bbe\u5907\u7c7b\u578b", "VENTILATOR");
        registryDeviceIdField = addField(form, row++, "\u8bbe\u5907ID", "");
        macAddressField = addField(form, row++, "MAC", "");
        sourceIpField = addField(form, row++, "\u6765\u6e90IP", "");
        sourcePortField = addField(form, row++, "\u6765\u6e90\u7aef\u53e3", "");
        deviceListenPortField = addField(form, row++, "\u76d1\u542c\u7aef\u53e3", valueOrDefault(config.get("hl7ListenerPort"), "5555"));
        nicknameField = addField(form, row++, "\u6635\u79f0", "");
        wardNameField = addField(form, row++, "\u75c5\u623f", "");
        bedNameField = addField(form, row++, "\u75c5\u5e8a", "");
        patientIdField = addField(form, row++, "\u60a3\u8005ID", "");
        patientNameField = addField(form, row++, "\u60a3\u8005\u59d3\u540d", "");
        patientNoField = addField(form, row++, "\u60a3\u8005\u7f16\u53f7", "");
        admissionNoField = addField(form, row++, "\u4f4f\u9662\u6d41\u6c34", "");
        Label ackLabel = new Label("ACK \u6a21\u5f0f");
        ackModeCombo = new ComboBox<>(FXCollections.observableArrayList("REQUIRED", "OPTIONAL", "DISABLED"));
        ackModeCombo.setValue("OPTIONAL");
        form.add(ackLabel, 0, row);
        form.add(ackModeCombo, 1, row++);
        deviceEnabledCheck = new CheckBox("\u542f\u7528\u8bbe\u5907");
        deviceEnabledCheck.setSelected(true);
        form.add(deviceEnabledCheck, 1, row);

        Button newBtn = new Button("\u65b0\u5efa");
        newBtn.setOnAction(e -> clearDeviceForm());
        Button saveBtn = new Button("\u4fdd\u5b58\u8bbe\u5907");
        saveBtn.setOnAction(e -> saveDevice());
        Button reloadBtn = new Button("\u5237\u65b0\u5217\u8868");
        reloadBtn.setOnAction(e -> refreshDevicesAsync());

        VBox box = new VBox(8, new HBox(8, newBtn, saveBtn, reloadBtn), deviceTable, form);
        VBox.setVgrow(box, Priority.ALWAYS);
        return box;
    }

    private VBox buildRecordTabCn() {
        recordDeviceFilter = new ComboBox<>();
        recordDeviceFilter.setConverter(new StringConverter<>() {
            @Override
            public String toString(DeviceRegistry object) {
                return object == null ? "\u5168\u90e8\u8bbe\u5907" : object.getDisplayName();
            }

            @Override
            public DeviceRegistry fromString(String string) {
                return null;
            }
        });
        patientKeywordField = new TextField();
        patientKeywordField.setPromptText("\u60a3\u8005\u59d3\u540d/ID");
        recordLimitField = new TextField("200");

        Button queryBtn = new Button("\u67e5\u8be2");
        queryBtn.setOnAction(e -> queryRecordsAsync());
        Button exportCsvBtn = new Button("\u5bfc\u51fa CSV");
        exportCsvBtn.setOnAction(e -> exportCurrentRecordsWithFeedback("CSV"));
        Button exportXlsxBtn = new Button("\u5bfc\u51fa XLSX");
        exportXlsxBtn.setOnAction(e -> exportCurrentRecordsWithFeedback("XLSX"));
        Button exportRawBtn = new Button("\u5bfc\u51fa\u539f\u59cb HL7");
        exportRawBtn.setOnAction(e -> exportSelectedRaw());

        recordTable = new TableView<>();
        recordTable.getColumns().add(column("\u63a5\u6536\u65f6\u95f4", r -> text(r.getReceiveTime())));
        recordTable.getColumns().add(column("\u8bbe\u5907", MedicalDataRecord::getDeviceName));
        recordTable.getColumns().add(column("\u8bbe\u5907\u7c7b\u578b", MedicalDataRecord::getDeviceType));
        recordTable.getColumns().add(column("\u60a3\u8005", MedicalDataRecord::getPatientName));
        recordTable.getColumns().add(column("\u75c5\u623f/\u75c5\u5e8a", r -> joinText(r.getWardName(), r.getBedName(), " / ")));
        recordTable.getColumns().add(column("\u6d88\u606f", r -> joinText(r.getMessageType(), r.getTriggerEvent(), "^")));
        recordTable.getColumns().add(column("\u5173\u952e\u6307\u6807", MedicalDataRecord::getSummaryText, 360));
        recordTable.getColumns().add(column("\u63a7\u5236\u53f7", MedicalDataRecord::getMessageControlId));
        recordTable.getColumns().add(column("\u72b6\u6001", MedicalDataRecord::getRecordStatus));
        recordTable.getSelectionModel().selectedItemProperty().addListener((obs, old, value) -> loadRecordDetail(value));
        VBox.setVgrow(recordTable, Priority.ALWAYS);
        detailArea = new TextArea();
        detailArea.setEditable(false);
        detailArea.setWrapText(true);
        rawArea = new TextArea();
        rawArea.setEditable(false);
        rawArea.setWrapText(false);

        HBox filterBox = new HBox(8,
            new Label("\u8bbe\u5907"), recordDeviceFilter,
            new Label("\u60a3\u8005"), patientKeywordField,
            new Label("\u6761\u6570"), recordLimitField,
            queryBtn, exportCsvBtn, exportXlsxBtn, exportRawBtn
        );
        SplitPane detailSplit = new SplitPane(detailArea, rawArea);
        detailSplit.setDividerPositions(0.45);
        VBox box = new VBox(8, filterBox, recordTable, detailSplit);
        VBox.setVgrow(box, Priority.ALWAYS);
        VBox.setVgrow(detailSplit, Priority.ALWAYS);
        return box;
    }

    private void logoutToLoginScene() {
        if (syncScheduler != null) {
            syncScheduler.stop();
        }
        stopListeners();
        appState.clearSession();
        mainScene = null;
        showLoginScene("已退出登录，请重新登录。");
    }

    private VBox buildStatusBox() {
        tokenLabel = new Label();
        orgLabel = new Label();
        listenerLabel = new Label();
        licenseLabel = new Label();
        summaryLabel = new Label();
        return new VBox(4, tokenLabel, orgLabel, listenerLabel, licenseLabel, summaryLabel);
    }

    private GridPane buildConfigBox() {
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(8);
        int row = 0;
        ssoBaseUrlField = addField(grid, row++, "ssoBaseUrl", config.get("ssoBaseUrl"));
        ssoAppIdField = addField(grid, row++, "ssoAppId", config.get("ssoAppId"));
        appBaseUrlField = addField(grid, row++, "appBaseUrl", config.get("appBaseUrl"));
        serverApiBaseField = addField(grid, row++, "serverClientApiBase", config.get("serverClientApiBase"));
        clientCodeField = addField(grid, row++, "clientCode", config.get("clientCode"));
        orgCodeField = addField(grid, row++, "orgCode", appState.getCurrentOrgCode());
        deviceIdField = addField(grid, row++, "deviceId(兼容)", config.get("deviceId"));
        listenerPortField = addField(grid, row++, "hl7ListenerPort", config.get("hl7ListenerPort"));
        syncIntervalField = addField(grid, row++, "syncIntervalSec", config.get("syncIntervalSec"));
        heartbeatIntervalField = addField(grid, row++, "heartbeatIntervalSec", config.get("heartbeatIntervalSec"));
        exportDirField = addField(grid, row, "exportDir", config.get("exportDir"));
        return grid;
    }

    private HBox buildActionBox() {
        Button saveBtn = new Button("保存配置");
        saveBtn.setOnAction(e -> saveConfig());

        Button loginBtn = new Button("SSO 登录");
        loginBtn.setOnAction(e -> loginSso());

        Button refreshOrgBtn = new Button("刷新机构");
        refreshOrgBtn.setOnAction(e -> refreshOrg());

        Button startBtn = new Button("启动监听");
        startBtn.setOnAction(e -> startListeners());

        Button stopBtn = new Button("停止监听");
        stopBtn.setOnAction(e -> stopListeners());

        Button syncBtn = new Button("立即同步");
        syncBtn.setOnAction(e -> syncScheduler.triggerSyncNow());

        Button logoutBtn = new Button("Logout");
        logoutBtn.setOnAction(e -> logoutToLoginScene());
        return new HBox(8, saveBtn, loginBtn, refreshOrgBtn, startBtn, stopBtn, syncBtn, logoutBtn);
    }

    private VBox buildDeviceTab() {
        deviceTable = new TableView<>();
        deviceTable.getColumns().add(column("设备ID", DeviceRegistry::getDeviceId));
        deviceTable.getColumns().add(column("昵称", DeviceRegistry::getDisplayName));
        deviceTable.getColumns().add(column("类型", DeviceRegistry::getDeviceType));
        deviceTable.getColumns().add(column("来源IP", DeviceRegistry::getSourceIp));
        deviceTable.getColumns().add(column("监听端口", d -> String.valueOf(d.getListenPort())));
        deviceTable.getColumns().add(column("病房/病床", d -> joinText(d.getWardName(), d.getBedName(), " / ")));
        deviceTable.getColumns().add(column("患者", DeviceRegistry::getPatientName));
        deviceTable.getColumns().add(column("ACK", DeviceRegistry::getAckMode));
        deviceTable.getColumns().add(column("启用", d -> d.isEnabled() ? "是" : "否"));
        deviceTable.getColumns().add(column("最近收到", d -> text(d.getLastSeenAt())));
        deviceTable.getSelectionModel().selectedItemProperty().addListener((obs, old, value) -> fillDeviceForm(value));
        VBox.setVgrow(deviceTable, Priority.ALWAYS);

        GridPane form = new GridPane();
        form.setHgap(10);
        form.setVgap(8);
        int row = 0;
        deviceTypeField = addField(form, row++, "设备类型", "VENTILATOR");
        registryDeviceIdField = addField(form, row++, "设备ID", "");
        macAddressField = addField(form, row++, "MAC", "");
        sourceIpField = addField(form, row++, "来源IP", "");
        sourcePortField = addField(form, row++, "来源端口", "");
        deviceListenPortField = addField(form, row++, "监听端口", valueOrDefault(config.get("hl7ListenerPort"), "5555"));
        nicknameField = addField(form, row++, "昵称", "");
        wardNameField = addField(form, row++, "病房", "");
        bedNameField = addField(form, row++, "病床", "");
        patientIdField = addField(form, row++, "患者ID", "");
        patientNameField = addField(form, row++, "患者姓名", "");
        patientNoField = addField(form, row++, "患者编号", "");
        admissionNoField = addField(form, row++, "住院流水", "");
        Label ackLabel = new Label("ACK 模式");
        ackModeCombo = new ComboBox<>(FXCollections.observableArrayList("REQUIRED", "OPTIONAL", "DISABLED"));
        ackModeCombo.setValue("OPTIONAL");
        form.add(ackLabel, 0, row);
        form.add(ackModeCombo, 1, row++);
        deviceEnabledCheck = new CheckBox("启用设备");
        deviceEnabledCheck.setSelected(true);
        form.add(deviceEnabledCheck, 1, row);

        Button newBtn = new Button("新建");
        newBtn.setOnAction(e -> clearDeviceForm());
        Button saveBtn = new Button("保存设备");
        saveBtn.setOnAction(e -> saveDevice());
        Button reloadBtn = new Button("刷新列表");
        reloadBtn.setOnAction(e -> refreshDevicesAsync());

        VBox box = new VBox(8, new HBox(8, newBtn, saveBtn, reloadBtn), deviceTable, form);
        VBox.setVgrow(box, Priority.ALWAYS);
        return box;
    }

    private VBox buildRecordTab() {
        recordDeviceFilter = new ComboBox<>();
        recordDeviceFilter.setConverter(new StringConverter<>() {
            @Override
            public String toString(DeviceRegistry object) {
                return object == null ? "全部设备" : object.getDisplayName();
            }

            @Override
            public DeviceRegistry fromString(String string) {
                return null;
            }
        });
        patientKeywordField = new TextField();
        patientKeywordField.setPromptText("患者姓名/ID");
        recordLimitField = new TextField("200");

        Button queryBtn = new Button("查询");
        queryBtn.setOnAction(e -> queryRecordsAsync());
        Button exportCsvBtn = new Button("导出 CSV");
        exportCsvBtn.setOnAction(e -> exportCurrentRecordsWithFeedback("CSV"));
        Button exportXlsxBtn = new Button("导出 XLSX");
        exportXlsxBtn.setOnAction(e -> exportCurrentRecordsWithFeedback("XLSX"));
        Button exportRawBtn = new Button("导出原始 HL7");
        exportRawBtn.setOnAction(e -> exportSelectedRaw());

        recordTable = new TableView<>();
        recordTable.getColumns().add(column("接收时间", r -> text(r.getReceiveTime())));
        recordTable.getColumns().add(column("设备", MedicalDataRecord::getDeviceName));
        recordTable.getColumns().add(column("设备类型", MedicalDataRecord::getDeviceType));
        recordTable.getColumns().add(column("患者", MedicalDataRecord::getPatientName));
        recordTable.getColumns().add(column("病房/病床", r -> joinText(r.getWardName(), r.getBedName(), " / ")));
        recordTable.getColumns().add(column("消息", r -> joinText(r.getMessageType(), r.getTriggerEvent(), "^")));
        recordTable.getColumns().add(column("控制号", MedicalDataRecord::getMessageControlId));
        recordTable.getColumns().add(column("状态", MedicalDataRecord::getRecordStatus));
        recordTable.getSelectionModel().selectedItemProperty().addListener((obs, old, value) -> loadRecordDetail(value));
        VBox.setVgrow(recordTable, Priority.ALWAYS);
        detailArea = new TextArea();
        detailArea.setEditable(false);
        detailArea.setWrapText(true);
        rawArea = new TextArea();
        rawArea.setEditable(false);
        rawArea.setWrapText(false);

        HBox filterBox = new HBox(8,
            new Label("设备"), recordDeviceFilter,
            new Label("患者"), patientKeywordField,
            new Label("条数"), recordLimitField,
            queryBtn, exportCsvBtn, exportXlsxBtn, exportRawBtn
        );
        SplitPane detailSplit = new SplitPane(detailArea, rawArea);
        detailSplit.setDividerPositions(0.45);
        VBox box = new VBox(8, filterBox, recordTable, detailSplit);
        VBox.setVgrow(box, Priority.ALWAYS);
        VBox.setVgrow(detailSplit, Priority.ALWAYS);
        return box;
    }

    private VBox buildLogTab() {
        logArea = new TextArea();
        logArea.setEditable(false);
        logArea.setWrapText(true);
        VBox box = new VBox(logArea);
        VBox.setVgrow(logArea, Priority.ALWAYS);
        return box;
    }

    private TextField addField(GridPane grid, int row, String label, String value) {
        Label l = new Label(label);
        TextField field = new TextField(value == null ? "" : value);
        grid.add(l, 0, row);
        grid.add(field, 1, row);
        GridPane.setHgrow(field, Priority.ALWAYS);
        return field;
    }

    private <T> TableColumn<T, String> column(String title, java.util.function.Function<T, String> mapper) {
        return column(title, mapper, 120);
    }

    private <T> TableColumn<T, String> column(String title,
                                              java.util.function.Function<T, String> mapper,
                                              double width) {
        TableColumn<T, String> col = new TableColumn<>(title);
        col.setCellValueFactory(cell -> new ReadOnlyStringWrapper(valueOrDefault(mapper.apply(cell.getValue()), "")));
        col.setPrefWidth(width);
        return col;
    }

    private void saveConfig() {
        config.set("ssoBaseUrl", ssoBaseUrlField.getText());
        config.set("ssoAppId", ssoAppIdField.getText());
        config.set("appBaseUrl", appBaseUrlField.getText());
        config.set("serverClientApiBase", serverApiBaseField.getText());
        config.set("clientCode", clientCodeField.getText());
        config.set("orgCode", orgCodeField.getText());
        config.set("deviceId", deviceIdField.getText());
        config.set("hl7ListenerPort", listenerPortField.getText());
        config.set("syncIntervalSec", syncIntervalField.getText());
        config.set("heartbeatIntervalSec", heartbeatIntervalField.getText());
        config.set("exportDir", exportDirField.getText());
        try {
            config.saveExternal();
            appendLog("配置已保存");
            store.saveRuntimeEvent("CONFIG", "INFO", "ui", "Config saved", null);
        } catch (Exception ex) {
            appendLog("配置保存失败: " + ex.getMessage());
            store.saveRuntimeEvent("CONFIG", "ERROR", "ui", ex.getMessage(), null);
        }
    }

    private void loginSso() {
        logoutToLoginScene();
    }

    private void refreshOrg() {
        ioExecutor.submit(() -> {
            appState.refreshCurrentOrg(this::appendLog);
            Platform.runLater(() -> {
                orgCodeField.setText(appState.getCurrentOrgCode());
                refreshStatusLabels();
            });
        });
    }

    private void startListeners() {
        ioExecutor.submit(() -> {
            List<Integer> ports = repository.listActiveListenPorts(appState.getCurrentOrgCode(), parseInt(listenerPortField.getText(), 5555));
            listenerManager.start(ports);
            store.saveRuntimeEvent("MLLP", "INFO", "listener", "Listeners started " + ports, null);
            appendLog("监听已启动: " + ports);
            Platform.runLater(this::refreshStatusLabels);
        });
    }

    private void stopListeners() {
        if (listenerManager != null) {
            listenerManager.stop();
            store.saveRuntimeEvent("MLLP", "INFO", "listener", "Listeners stopped", null);
        }
        Platform.runLater(this::refreshStatusLabels);
    }

    private String handleHl7Message(MllpServer.MessageContext context) {
        String orgCode = appState.getCurrentOrgCode();
        try {
            Hl7MessageParser.ParseResult result = parser.parseOru(context.getHl7Message(), "INMEHL7CLIENT", orgCode);
            store.saveRawMessage(context.getHl7Message(), result.isSuccess(), result.getErrorMessage());
            if (result.isSuccess()) {
                Hl7ResultRecord record = result.getRecord();
                DeviceRegistry device = repository.resolveOrCreateDevice(orgCode, context.getListenPort(), context.getRemoteIp(), context.getRemotePort(), record);
                repository.saveInboundRecord(orgCode, device, context, record, result.isAccepted(), result.getErrorMessage());
                store.saveResult(config.get("clientCode"), orgCode, device == null ? config.get("deviceId") : device.getDeviceId(), record);
                appendLog("收到 HL7 数据: device=" + (device == null ? "-" : device.getDisplayName()) + ", controlId=" + record.getMessageControlId());
            } else {
                repository.saveInboundRecord(orgCode, null, context, null, result.isAccepted(), result.getErrorMessage());
                appendLog("HL7 解析拒绝: " + result.getErrorMessage());
            }
            Platform.runLater(() -> {
                refreshStatusLabels();
                refreshDevicesAsync();
                queryRecordsAsync();
            });
            return result.getAckMessage();
        } catch (Exception ex) {
            logger.error("handleHl7Message failed", ex);
            appendLog("处理 HL7 报文失败: " + ex.getMessage());
            repository.saveInboundRecord(orgCode, null, context, null, false, ex.getMessage());
            return null;
        }
    }

    private void refreshDevicesAsync() {
        ioExecutor.submit(() -> {
            List<DeviceRegistry> devices = repository.listDevices(appState.getCurrentOrgCode());
            Platform.runLater(() -> {
                deviceTable.setItems(FXCollections.observableArrayList(devices));
                refreshRecordDeviceFilter(devices);
                refreshStatusLabels();
            });
        });
    }

    private void refreshRecordDeviceFilter(List<DeviceRegistry> devices) {
        DeviceRegistry selected = recordDeviceFilter.getValue();
        List<DeviceRegistry> values = new ArrayList<>();
        values.add(null);
        values.addAll(devices);
        recordDeviceFilter.setItems(FXCollections.observableArrayList(values));
        if (selected != null) {
            for (DeviceRegistry device : devices) {
                if (device != null && selected.getDeviceId() != null && selected.getDeviceId().equals(device.getDeviceId())) {
                    recordDeviceFilter.setValue(device);
                    return;
                }
            }
        }
        recordDeviceFilter.setValue(null);
    }

    private void saveDevice() {
        DeviceRegistry base = deviceTable.getSelectionModel().getSelectedItem();
        DeviceRegistry device = base == null ? new DeviceRegistry() : base;
        device.setOrgCode(appState.getCurrentOrgCode());
        device.setDeviceType(deviceTypeField.getText());
        device.setDeviceId(registryDeviceIdField.getText());
        device.setMacAddress(macAddressField.getText());
        device.setSourceIp(sourceIpField.getText());
        device.setSourcePort(parseInt(sourcePortField.getText(), 0));
        device.setListenPort(parseInt(deviceListenPortField.getText(), parseInt(listenerPortField.getText(), 5555)));
        device.setNickname(nicknameField.getText());
        device.setWardName(wardNameField.getText());
        device.setBedName(bedNameField.getText());
        device.setPatientId(patientIdField.getText());
        device.setPatientName(patientNameField.getText());
        device.setPatientNo(patientNoField.getText());
        device.setAdmissionNo(admissionNoField.getText());
        device.setAckMode(ackModeCombo.getValue());
        device.setEnabled(deviceEnabledCheck.isSelected());
        device.setBindSource("LOCAL");

        ioExecutor.submit(() -> {
            DeviceRegistry saved = repository.saveDevice(device);
            appendLog("设备已保存: " + (saved == null ? device.getDeviceId() : saved.getDisplayName()));
            refreshDevicesAsync();
            if (listenerManager != null && listenerManager.isRunning()) {
                startListeners();
            }
        });
    }

    private void clearDeviceForm() {
        deviceTable.getSelectionModel().clearSelection();
        deviceTypeField.setText("VENTILATOR");
        registryDeviceIdField.clear();
        macAddressField.clear();
        sourceIpField.clear();
        sourcePortField.clear();
        deviceListenPortField.setText(valueOrDefault(listenerPortField.getText(), "5555"));
        nicknameField.clear();
        wardNameField.clear();
        bedNameField.clear();
        patientIdField.clear();
        patientNameField.clear();
        patientNoField.clear();
        admissionNoField.clear();
        ackModeCombo.setValue("OPTIONAL");
        deviceEnabledCheck.setSelected(true);
    }

    private void fillDeviceForm(DeviceRegistry device) {
        if (device == null) {
            return;
        }
        deviceTypeField.setText(valueOrDefault(device.getDeviceType(), ""));
        registryDeviceIdField.setText(valueOrDefault(device.getDeviceId(), ""));
        macAddressField.setText(valueOrDefault(device.getMacAddress(), ""));
        sourceIpField.setText(valueOrDefault(device.getSourceIp(), ""));
        sourcePortField.setText(device.getSourcePort() > 0 ? String.valueOf(device.getSourcePort()) : "");
        deviceListenPortField.setText(device.getListenPort() > 0 ? String.valueOf(device.getListenPort()) : "");
        nicknameField.setText(valueOrDefault(device.getNickname(), ""));
        wardNameField.setText(valueOrDefault(device.getWardName(), ""));
        bedNameField.setText(valueOrDefault(device.getBedName(), ""));
        patientIdField.setText(valueOrDefault(device.getPatientId(), ""));
        patientNameField.setText(valueOrDefault(device.getPatientName(), ""));
        patientNoField.setText(valueOrDefault(device.getPatientNo(), ""));
        admissionNoField.setText(valueOrDefault(device.getAdmissionNo(), ""));
        ackModeCombo.setValue(valueOrDefault(device.getAckMode(), "OPTIONAL"));
        deviceEnabledCheck.setSelected(device.isEnabled());
    }

    private void queryRecordsAsync() {
        String deviceId = recordDeviceFilter == null || recordDeviceFilter.getValue() == null
            ? null : recordDeviceFilter.getValue().getDeviceId();
        String keyword = patientKeywordField == null ? null : patientKeywordField.getText();
        int limit = parseInt(recordLimitField == null ? null : recordLimitField.getText(), 200);
        ioExecutor.submit(() -> {
            List<MedicalDataRecord> records = repository.queryRecords(appState.getCurrentOrgCode(), deviceId, keyword, limit);
            currentRecords = records;
            Platform.runLater(() -> {
                recordTable.setItems(FXCollections.observableArrayList(records));
                if (!records.isEmpty()) {
                    recordTable.getSelectionModel().selectFirst();
                } else {
                    detailArea.clear();
                    rawArea.clear();
                }
                refreshStatusLabels();
            });
        });
    }

    private void loadRecordDetail(MedicalDataRecord summary) {
        if (summary == null) {
            detailArea.clear();
            rawArea.clear();
            return;
        }
        ioExecutor.submit(() -> {
            MedicalDataRecord detail = repository.loadRecord(summary.getId());
            Platform.runLater(() -> {
                if (detail == null) {
                    detailArea.setText("未找到记录详情");
                    rawArea.clear();
                    return;
                }
                detailArea.setText(formatRecordDetailEnhanced(detail));
                rawArea.setText(valueOrDefault(detail.getRawPayload(), ""));
            });
        });
    }

    private String formatRecordDetail(MedicalDataRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append("设备: ").append(valueOrDefault(record.getDeviceName(), record.getDeviceId())).append(System.lineSeparator());
        sb.append("设备类型: ").append(valueOrDefault(record.getDeviceType(), "-")).append(System.lineSeparator());
        sb.append("患者: ").append(joinText(record.getPatientName(), record.getPatientId(), " / ")).append(System.lineSeparator());
        sb.append("病房病床: ").append(joinText(record.getWardName(), record.getBedName(), " / ")).append(System.lineSeparator());
        sb.append("消息: ").append(joinText(record.getMessageType(), record.getTriggerEvent(), "^")).append(System.lineSeparator());
        sb.append("控制号: ").append(valueOrDefault(record.getMessageControlId(), "-")).append(System.lineSeparator());
        sb.append("报告时间: ").append(text(record.getReportTime())).append(System.lineSeparator());
        sb.append(System.lineSeparator()).append("指标明细").append(System.lineSeparator());
        for (MedicalDataItem item : record.getItems()) {
            sb.append("- ")
                .append(valueOrDefault(item.getItemName(), item.getItemCode()))
                .append(": ")
                .append(valueOrDefault(item.getValueText(), ""))
                .append(" ")
                .append(valueOrDefault(item.getUnit(), ""))
                .append(" [")
                .append(valueOrDefault(item.getAbnormalFlag(), ""))
                .append("]")
                .append(System.lineSeparator());
        }
        return sb.toString();
    }

    private String formatRecordDetailEnhanced(MedicalDataRecord record) {
        VentilatorRecordSupport.Snapshot snapshot = VentilatorRecordSupport.buildSnapshot(record);
        StringBuilder sb = new StringBuilder();
        sb.append("设备: ").append(valueOrDefault(record.getDeviceName(), record.getDeviceId())).append(System.lineSeparator());
        sb.append("设备类型: ").append(valueOrDefault(record.getDeviceType(), "-")).append(System.lineSeparator());
        sb.append("患者: ").append(joinText(record.getPatientName(), record.getPatientId(), " / ")).append(System.lineSeparator());
        sb.append("病房病床: ").append(joinText(record.getWardName(), record.getBedName(), " / ")).append(System.lineSeparator());
        sb.append("消息: ").append(joinText(record.getMessageType(), record.getTriggerEvent(), "^")).append(System.lineSeparator());
        sb.append("控制号: ").append(valueOrDefault(record.getMessageControlId(), "-")).append(System.lineSeparator());
        sb.append("报告时间: ").append(text(record.getReportTime())).append(System.lineSeparator());
        String summary = VentilatorRecordSupport.buildSummary(snapshot);
        if (!valueOrDefault(summary, "").isBlank()) {
            sb.append("关键指标: ").append(summary).append(System.lineSeparator());
        }
        String detailBlock = VentilatorRecordSupport.buildDetailBlock(snapshot);
        if (!valueOrDefault(detailBlock, "").isBlank()) {
            sb.append(System.lineSeparator()).append(detailBlock).append(System.lineSeparator());
        }
        sb.append(System.lineSeparator()).append("原始解析指标").append(System.lineSeparator());
        for (MedicalDataItem item : record.getItems()) {
            sb.append("- ")
                .append(valueOrDefault(item.getItemName(), item.getItemCode()))
                .append(": ")
                .append(valueOrDefault(item.getValueText(), ""))
                .append(" ")
                .append(valueOrDefault(item.getUnit(), ""))
                .append(" [")
                .append(valueOrDefault(item.getAbnormalFlag(), ""))
                .append("]")
                .append(System.lineSeparator());
        }
        return sb.toString();
    }

    private void exportCurrentRecordsWithFeedback(String type) {
        if (currentRecords == null || currentRecords.isEmpty()) {
            appendLog("当前没有可导出的记录");
            Platform.runLater(() -> showExportFailure("当前没有可导出的记录"));
            return;
        }
        ioExecutor.submit(() -> {
            try {
                List<MedicalDataRecord> details = loadCurrentRecordDetails();
                Path dir = resolveExportDir();
                String ts = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now());
                Path target;
                if ("XLSX".equalsIgnoreCase(type) && isVentilatorExport(details)) {
                    target = dir.resolve("呼吸功能监测表-联泰-" + ts + ".xlsx");
                    exportService.exportVentilatorTemplate(
                        details,
                        target,
                        resolveVentilatorTemplatePath(),
                        appState.getCurrentOrgInfo() == null ? null : appState.getCurrentOrgInfo().getOrgName()
                    );
                } else if ("XLSX".equalsIgnoreCase(type)) {
                    target = dir.resolve("hl7-records-" + ts + ".xlsx");
                    exportService.exportXlsx(details, target);
                } else {
                    target = dir.resolve("hl7-records-" + ts + ".csv");
                    exportService.exportCsv(details, target);
                }
                repository.saveExportTask(
                    appState.getCurrentOrgCode(),
                    recordDeviceFilter.getValue() == null ? null : recordDeviceFilter.getValue().getDeviceId(),
                    type,
                    minRecordTime(details),
                    maxRecordTime(details),
                    details.size(),
                    target.toString(),
                    appState.getCurrentOrgInfo() == null ? null : appState.getCurrentOrgInfo().getRealName()
                );
                appendLog("导出完成: " + target);
                Platform.runLater(() -> showExportSuccess(target));
            } catch (Exception ex) {
                appendLog("导出失败: " + ex.getMessage());
                Platform.runLater(() -> showExportFailure(ex.getMessage()));
            }
        });
    }

    private void exportCurrentRecords(String type) {
        if (currentRecords == null || currentRecords.isEmpty()) {
            appendLog("当前没有可导出的记录");
            return;
        }
        ioExecutor.submit(() -> {
            try {
                List<MedicalDataRecord> details = loadCurrentRecordDetails();
                Path dir = resolveExportDir();
                String ts = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now());
                Path target;
                if ("XLSX".equalsIgnoreCase(type) && isVentilatorExport(details)) {
                    target = dir.resolve("呼吸功能监测表-联泰-" + ts + ".xlsx");
                    exportService.exportVentilatorTemplate(
                        details,
                        target,
                        resolveVentilatorTemplatePath(),
                        appState.getCurrentOrgInfo() == null ? null : appState.getCurrentOrgInfo().getOrgName()
                    );
                } else if ("XLSX".equalsIgnoreCase(type)) {
                    target = dir.resolve("hl7-records-" + ts + ".xlsx");
                    exportService.exportXlsx(details, target);
                } else {
                    target = dir.resolve("hl7-records-" + ts + ".csv");
                    exportService.exportCsv(details, target);
                }
                repository.saveExportTask(
                    appState.getCurrentOrgCode(),
                    recordDeviceFilter.getValue() == null ? null : recordDeviceFilter.getValue().getDeviceId(),
                    type,
                    minRecordTime(details),
                    maxRecordTime(details),
                    details.size(),
                    target.toString(),
                    appState.getCurrentOrgInfo() == null ? null : appState.getCurrentOrgInfo().getRealName()
                );
                appendLog("导出完成: " + target);
            } catch (Exception ex) {
                appendLog("导出失败: " + ex.getMessage());
            }
        });
    }

    private boolean isVentilatorExport(List<MedicalDataRecord> details) {
        if (details == null || details.isEmpty()) {
            return false;
        }
        for (MedicalDataRecord record : details) {
            if (!VentilatorRecordSupport.isVentilatorRecord(record)) {
                return false;
            }
        }
        return true;
    }

    private Path resolveVentilatorTemplatePath() {
        String fallback = "C:/CodeWorkspace/zhunyintech-code-role/projects/inme-hl7/项目文档/呼吸功能监测表-联泰.xlsx";
        String configured = valueOrDefault(
            config.get("ventilatorExportTemplatePath"),
            fallback
        );
        Path configuredPath = Paths.get(configured).toAbsolutePath().normalize();
        if (configuredPath.toFile().exists()) {
            return configuredPath;
        }
        return Paths.get(fallback).toAbsolutePath().normalize();
    }

    private void exportSelectedRaw() {
        MedicalDataRecord selected = recordTable.getSelectionModel().getSelectedItem();
        if (selected == null) {
            appendLog("请先选择一条记录");
            return;
        }
        ioExecutor.submit(() -> {
            try {
                MedicalDataRecord detail = repository.loadRecord(selected.getId());
                if (detail == null) {
                    appendLog("未找到待导出的原始记录");
                    return;
                }
                Path dir = resolveExportDir();
                String fileName = "hl7-raw-" + valueOrDefault(detail.getMessageControlId(), String.valueOf(detail.getId())) + ".txt";
                Path target = dir.resolve(fileName);
                exportService.exportRawTxt(detail, target);
                repository.saveExportTask(
                    appState.getCurrentOrgCode(),
                    detail.getDeviceId(),
                    "TXT",
                    detail.getReceiveTime(),
                    detail.getReceiveTime(),
                    1,
                    target.toString(),
                    appState.getCurrentOrgInfo() == null ? null : appState.getCurrentOrgInfo().getRealName()
                );
                appendLog("原始 HL7 已导出: " + target);
            } catch (Exception ex) {
                appendLog("导出原始 HL7 失败: " + ex.getMessage());
            }
        });
    }

    private void showExportSuccess(Path target) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("导出完成");
        alert.setHeaderText("文件已导出");
        alert.setContentText(target == null ? "" : target.toAbsolutePath().toString());
        alert.showAndWait();
        openExportLocation(target);
    }

    private void showExportFailure(String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("导出失败");
        alert.setHeaderText("未能生成导出文件");
        alert.setContentText(valueOrDefault(message, "未知错误"));
        alert.showAndWait();
    }

    private void openExportLocation(Path target) {
        if (target == null) {
            return;
        }
        try {
            Path file = target.toAbsolutePath().normalize();
            if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
                new ProcessBuilder("explorer.exe", "/select," + file).start();
                return;
            }
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().open(file.getParent().toFile());
            }
        } catch (Exception ex) {
            appendLog("打开导出目录失败: " + ex.getMessage());
        }
    }

    private List<MedicalDataRecord> loadCurrentRecordDetails() {
        List<MedicalDataRecord> details = new ArrayList<>();
        for (MedicalDataRecord record : currentRecords) {
            MedicalDataRecord detail = repository.loadRecord(record.getId());
            if (detail != null) {
                details.add(detail);
            }
        }
        return details;
    }

    private Path resolveExportDir() {
        String dir = exportDirField == null
            ? valueOrDefault(config.get("exportDir"), "./exports")
            : valueOrDefault(exportDirField.getText(), valueOrDefault(config.get("exportDir"), "./exports"));
        return Paths.get(dir).toAbsolutePath().normalize();
    }

    private LocalDateTime minRecordTime(List<MedicalDataRecord> records) {
        return records.stream()
            .map(MedicalDataRecord::getReceiveTime)
            .filter(v -> v != null)
            .min(Comparator.naturalOrder())
            .orElse(null);
    }

    private LocalDateTime maxRecordTime(List<MedicalDataRecord> records) {
        return records.stream()
            .map(MedicalDataRecord::getReceiveTime)
            .filter(v -> v != null)
            .max(Comparator.naturalOrder())
            .orElse(null);
    }

    private void refreshStatusLabels() {
        String token = appState == null ? null : appState.getToken();
        if (tokenLabel != null) {
            tokenLabel.setText("Token: " + (token == null || token.isEmpty() ? "未登录" : "已登录"));
        }
        if (orgLabel != null && appState != null) {
            OrgInfo orgInfo = appState.getCurrentOrgInfo();
            orgLabel.setText("机构: " + joinText(appState.getCurrentOrgCode(), orgInfo == null ? null : orgInfo.getOrgName(), " / "));
        }
        if (listenerLabel != null) {
            listenerLabel.setText("监听状态: " + (listenerManager == null ? "STOPPED" : listenerManager.describe()));
        }
        if (licenseLabel != null && appState != null) {
            LicenseSnapshot snapshot = appState.getLicenseSnapshot();
            licenseLabel.setText("License: " + valueOrDefault(snapshot.getLicenseStatus(), "-")
                + ", daysLeft=" + snapshot.getDaysLeft()
                + ", message=" + valueOrDefault(snapshot.getMessage(), "-"));
        }
        if (summaryLabel != null && repository != null && appState != null) {
            summaryLabel.setText("今日接收: " + repository.countTodayRecords(appState.getCurrentOrgCode())
                + " 条, 已启用设备: " + repository.countEnabledDevices(appState.getCurrentOrgCode()) + " 台");
        }
    }

    private void appendLog(String line) {
        if (line == null || line.trim().isEmpty()) {
            return;
        }
        Platform.runLater(() -> {
            if (loginStatusLabel != null) {
                loginStatusLabel.setText(line);
            }
            if (loginLogArea != null) {
                loginLogArea.appendText(line + System.lineSeparator());
            }
            if (logArea != null) {
                logArea.appendText(line + System.lineSeparator());
            }
            refreshStatusLabels();
        });
    }

    private int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value == null ? "" : value.trim());
        } catch (Exception ignored) {
            return defaultValue;
        }
    }

    private String text(LocalDateTime value) {
        return value == null ? "" : TS.format(value);
    }

    private String joinText(String left, String right, String joiner) {
        if ((left == null || left.isBlank()) && (right == null || right.isBlank())) {
            return "";
        }
        if (left == null || left.isBlank()) {
            return right.trim();
        }
        if (right == null || right.isBlank()) {
            return left.trim();
        }
        return left.trim() + joiner + right.trim();
    }

    private String valueOrDefault(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
