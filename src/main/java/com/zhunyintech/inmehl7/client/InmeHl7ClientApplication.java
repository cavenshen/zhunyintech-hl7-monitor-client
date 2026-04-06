package com.zhunyintech.inmehl7.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.zhunyintech.inmehl7.client.model.MonitorListenerProfile;
import com.zhunyintech.inmehl7.client.model.MonitorProtocolType;
import com.zhunyintech.inmehl7.client.model.OrgInfo;
import com.zhunyintech.inmehl7.client.serial.SerialProtocolListener;
import com.zhunyintech.inmehl7.client.state.AppState;
import com.zhunyintech.inmehl7.client.support.ClientMachineSupport;
import com.zhunyintech.inmehl7.client.sync.MonitorConfigApiClient;
import com.zhunyintech.inmehl7.client.sync.ServerApiClient;
import com.zhunyintech.inmehl7.client.sync.SyncScheduler;
import com.zhunyintech.inmehl7.client.ventilator.VentilatorRecordSupport;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Menu;
import javafx.scene.control.MenuBar;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SeparatorMenuItem;
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
import javafx.stage.DirectoryChooser;
import javafx.util.StringConverter;

import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.net.URLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InmeHl7ClientApplication extends Application {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ExecutorService ioExecutor = Executors.newCachedThreadPool();
    private final Hl7MessageParser parser = new Hl7MessageParser();
    private final DataExportService exportService = new DataExportService();
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    private Config config;
    private RuntimeLogger logger;
    private SQLiteStore store;
    private ClientRepository repository;
    private AppState appState;
    private ServerApiClient apiClient;
    private MonitorConfigApiClient monitorConfigApiClient;
    private SyncScheduler syncScheduler;
    private ListenerManager listenerManager;
    private SerialProtocolListener serialProtocolListener;
    private Stage primaryStage;
    private Scene loginScene;
    private Scene mainScene;
    private final AtomicBoolean uiLoginInProgress = new AtomicBoolean(false);
    private final Map<MonitorProtocolType, MonitorListenerProfile> listenerProfileCache = new EnumMap<>(MonitorProtocolType.class);
    private String localMacAddress;
    private String generatedClientCode;

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
    private ComboBox<OrgInfo> authorizedOrgCombo;
    private TextField deviceIdField;
    private TextField listenerPortField;
    private TextField syncIntervalField;
    private TextField heartbeatIntervalField;
    private TextField exportDirField;
    private ComboBox<MonitorProtocolType> protocolTypeCombo;
    private TextField serialPortNameField;
    private ComboBox<String> baudRateCombo;
    private ComboBox<String> dataBitsCombo;
    private ComboBox<String> stopBitsCombo;
    private ComboBox<String> parityCombo;
    private TextField readTimeoutField;
    private TextField pollIntervalField;
    private ComboBox<String> charsetCombo;
    private ComboBox<String> frameDelimiterCombo;
    private TextField stationNoField;
    private VBox protocolConfigBox;
    private Label configStatusLabel;
    private Button configSaveButton;
    private Button exportDirChooseButton;

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
        stage.setTitle("寅米医疗器械接入终端软件");
        if (appState.hasToken() || appState.hasCompletedLogin()) {
            appState.clearSession();
            appendLog("已清理本地登录状态，进入系统前请先完成浏览器 SSO 登录。");
        }
        showLoginScene("请点击下方按钮完成浏览器登录。");
        stage.show();
        appendLog("客户端已启动，等待浏览器登录。");
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
        stopMonitorListeners();
        ioExecutor.shutdownNow();
    }

    private void initRuntime() {
        config = new Config();
        localMacAddress = ClientMachineSupport.resolveLocalMacAddress();
        generatedClientCode = ClientMachineSupport.buildClientCode(localMacAddress);
        config.set("clientCode", generatedClientCode);
        logger = new RuntimeLogger(config);
        logger.bindSink(this::appendLog);
        store = new SQLiteStore(config, logger);
        repository = new ClientRepository(config, logger);
        store.init();
        repository.init();
        appState = new AppState(config, store, repository, logger);
        apiClient = new ServerApiClient(config, logger);
        monitorConfigApiClient = new MonitorConfigApiClient(config, logger);
        listenerManager = new ListenerManager(logger, this::handleHl7Message);
        syncScheduler = new SyncScheduler(
            config, logger, store, appState, apiClient,
            this::isAnyListenerRunning,
            this::appendLog
        );
    }

    private void showLoginScene(String statusText) {
        if (syncScheduler != null) {
            syncScheduler.stop();
        }
        stopMonitorListeners();
        if (loginScene == null) {
            loginScene = buildLoginScene();
        }
        if (loginStatusLabel != null) {
            loginStatusLabel.setText(valueOrDefault(statusText, "请点击下方按钮完成浏览器登录。"));
        }
        if (primaryStage != null) {
            primaryStage.setScene(loginScene);
            primaryStage.setWidth(920);
            primaryStage.setHeight(560);
            primaryStage.setMinWidth(920);
            primaryStage.setMinHeight(560);
        }
    }

    private Scene buildLoginScene() {
        if (useSimpleLoginScene()) {
            return buildBrandedLoginScene();
        }
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

    private boolean useSimpleLoginScene() {
        return true;
    }

    private Scene buildSimpleLoginScene() {
        VBox root = new VBox(16);
        root.setPadding(new Insets(28));
        root.setAlignment(Pos.CENTER);

        loginStatusLabel = new Label("请先完成浏览器 SSO 登录，再进入客户端。");
        loginStatusLabel.setWrapText(true);
        loginStatusLabel.setMaxWidth(420);
        loginStatusLabel.setAlignment(Pos.CENTER);
        loginStatusLabel.setStyle("-fx-font-size: 14px;");

        loginButton = new Button("浏览器 SSO 登录");
        loginButton.setDefaultButton(true);
        loginButton.setPrefWidth(160);
        loginButton.setPrefHeight(36);
        loginButton.setOnAction(e -> loginFromLoginScene());

        root.getChildren().addAll(loginStatusLabel, loginButton);
        return new Scene(root, 560, 240);
    }

    private Scene buildBrandedLoginScene() {
        VBox root = new VBox();
        root.setAlignment(Pos.CENTER);
        root.setStyle("-fx-background-color: linear-gradient(to bottom right, #f5fbff, #e7f1fb 45%, #dce8f6 100%);");

        VBox card = new VBox(22);
        card.setAlignment(Pos.CENTER);
        card.setMaxWidth(620);
        card.setPadding(new Insets(42, 54, 42, 54));
        card.setStyle("-fx-background-color: rgba(255,255,255,0.96);"
            + "-fx-background-radius: 24;"
            + "-fx-border-radius: 24;"
            + "-fx-border-color: rgba(127,156,186,0.28);"
            + "-fx-border-width: 1;"
            + "-fx-effect: dropshadow(gaussian, rgba(35,74,120,0.16), 32, 0.18, 0, 12);");

        Label title = new Label("寅米医疗器械接入终端软件");
        title.setWrapText(true);
        title.setAlignment(Pos.CENTER);
        title.setStyle("-fx-font-size: 32px;"
            + "-fx-font-weight: bold;"
            + "-fx-text-fill: #16324f;");

        loginStatusLabel = new Label("请点击下方按钮完成浏览器登录。");
        loginStatusLabel.setWrapText(true);
        loginStatusLabel.setMaxWidth(520);
        loginStatusLabel.setAlignment(Pos.CENTER);
        loginStatusLabel.setStyle("-fx-font-size: 16px;"
            + "-fx-text-fill: #204161;");

        loginButton = new Button("浏览器登录");
        loginButton.setDefaultButton(true);
        loginButton.setPrefWidth(240);
        loginButton.setPrefHeight(46);
        loginButton.setStyle("-fx-background-color: linear-gradient(to bottom, #7fd1ff, #49b6f2);"
            + "-fx-background-radius: 12;"
            + "-fx-text-fill: #17324d;"
            + "-fx-font-size: 15px;"
            + "-fx-font-weight: bold;"
            + "-fx-cursor: hand;");
        loginButton.setOnAction(e -> loginFromLoginScene());

        card.getChildren().addAll(title, loginStatusLabel, loginButton);
        root.getChildren().add(card);
        return new Scene(root, 920, 560);
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
        setLoginUiBusy(true);
        setLoginStatusMessage("登录中,请稍后...");
        ioExecutor.submit(() -> {
            boolean ok = appState.loginInBrowser(this::handleLoginProgress);
            Platform.runLater(() -> {
                setLoginUiBusy(false);
                if (!ok || !appState.hasCompletedLogin()) {
                    if (loginStatusLabel != null
                        && (loginStatusLabel.getText() == null
                        || loginStatusLabel.getText().isBlank()
                        || "登录中,请稍后...".equals(loginStatusLabel.getText()))) {
                        loginStatusLabel.setText("登录失败，请稍后重试。");
                    }
                    return;
                }
                showMainSceneCn();
            });
        });
    }

    private void handleLoginProgress(String line) {
        appendLog(line);
        String status = mapLoginStatusMessage(line);
        if (status != null) {
            setLoginStatusMessage(status);
        }
    }

    private String mapLoginStatusMessage(String line) {
        if (line == null || line.isBlank()) {
            return null;
        }
        String text = line.trim();
        if (text.contains("业务端个人信息获取失败")
            || text.contains("业务端机构列表获取失败")
            || text.contains("机构详情获取失败")) {
            return "登录中,请稍后...";
        }
        if (text.contains("超时")) {
            return "登录超时，请重新点击浏览器登录。";
        }
        if (text.contains("失败")) {
            if (text.endsWith(": null") || text.endsWith("：null") || "失败".equals(text)) {
                return "登录失败，请检查网络或稍后重试。";
            }
            return text;
        }
        if (text.contains("拒绝")) {
            return "登录失败：" + text;
        }
        if (text.contains("未获取到 token") || text.contains("未获取到登录凭证")) {
            return "登录失败：未获取到登录凭证。";
        }
        if (text.contains("初始化机构信息")) {
            return "登录成功，正在初始化机构信息...";
        }
        return "登录中,请稍后...";
    }

    private void setLoginStatusMessage(String message) {
        if (message == null || message.isBlank()) {
            return;
        }
        Platform.runLater(() -> {
            if (loginStatusLabel != null) {
                loginStatusLabel.setText(message);
            }
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
        root.getChildren().add(buildMainMenuBarCn());
        root.getChildren().add(buildMonitorConfigPaneCn());

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
        refreshMonitorConfigUi();
        loadRemoteProfilesAsyncV2();
        refreshDevicesAsync();
        queryRecordsAsync();
        syncScheduler.start();
    }

    private MenuBar buildMainMenuBarCn() {
        Menu systemMenu = new Menu("系统");
        MenuItem saveConfigItem = new MenuItem("保存监听配置");
        saveConfigItem.setOnAction(e -> saveMonitorConfig());
        MenuItem reloginItem = new MenuItem("重新登录");
        reloginItem.setOnAction(e -> loginSso());
        MenuItem logoutItem = new MenuItem("退出登录");
        logoutItem.setOnAction(e -> logoutToLoginScene());
        systemMenu.getItems().addAll(saveConfigItem, new SeparatorMenuItem(), reloginItem, logoutItem);

        Menu listenerMenu = new Menu("设备监听");
        MenuItem startItem = new MenuItem("启动监听");
        startItem.setOnAction(e -> startMonitorListeners());
        MenuItem stopItem = new MenuItem("停止监听");
        stopItem.setOnAction(e -> stopMonitorListeners());
        MenuItem syncItem = new MenuItem("立即同步");
        syncItem.setOnAction(e -> {
            if (syncScheduler != null) {
                syncScheduler.triggerSyncNow();
            }
        });
        listenerMenu.getItems().addAll(startItem, stopItem, new SeparatorMenuItem(), syncItem);

        Menu aboutMenu = new Menu("关于");
        MenuItem versionItem = new MenuItem("当前版本");
        versionItem.setOnAction(e -> showAboutDialog());
        MenuItem upgradeItem = new MenuItem("版本升级");
        upgradeItem.setOnAction(e -> checkForUpdateAsync());
        aboutMenu.getItems().addAll(versionItem, upgradeItem);

        return new MenuBar(systemMenu, listenerMenu, aboutMenu);
    }

    private VBox buildMonitorConfigPaneCn() {
        VBox box = new VBox(10);
        box.setPadding(new Insets(14));
        box.setStyle("-fx-background-color: #ffffff; -fx-background-radius: 10; -fx-border-color: #d8e2ee; -fx-border-radius: 10;");

        Label title = new Label("监听参数配置");
        title.setStyle("-fx-font-size: 18px; -fx-font-weight: bold; -fx-text-fill: #16324f;");

        clientCodeField = new TextField(generatedClientCode);
        clientCodeField.setEditable(false);
        clientCodeField.setDisable(true);

        orgCodeField = new TextField(appState.getCurrentOrgCode());
        orgCodeField.setManaged(false);
        orgCodeField.setVisible(false);

        authorizedOrgCombo = new ComboBox<>();
        authorizedOrgCombo.setConverter(new StringConverter<>() {
            @Override
            public String toString(OrgInfo object) {
                if (object == null) {
                    return "";
                }
                return joinText(object.getOrgName(), object.getOrgCode(), " / ");
            }

            @Override
            public OrgInfo fromString(String string) {
                return null;
            }
        });
        authorizedOrgCombo.valueProperty().addListener((obs, old, value) -> {
            if (value == null) {
                return;
            }
            orgCodeField.setText(valueOrDefault(value.getOrgCode(), ""));
            appState.selectAuthorizedOrg(value.getOrgCode());
            MonitorListenerProfile profile = collectProfileFromForm();
            if (profile != null) {
                profile.setOrgCode(value.getOrgCode());
                profile.setOrgName(value.getOrgName());
                listenerProfileCache.put(profile.getProtocolType(), profile);
            }
            refreshDevicesAsync();
            queryRecordsAsync();
        });

        protocolTypeCombo = new ComboBox<>(FXCollections.observableArrayList(MonitorProtocolType.values()));
        protocolTypeCombo.valueProperty().addListener((obs, old, value) -> {
            if (value == null) {
                return;
            }
            if (old != null) {
                MonitorListenerProfile oldProfile = collectProfileFromForm();
                if (oldProfile != null) {
                    listenerProfileCache.put(old, oldProfile);
                }
            }
            applyProfileToForm(getOrCreateProfile(value));
            updateConfigEditStateV2();
        });

        syncIntervalField = new TextField();
        heartbeatIntervalField = new TextField();
        exportDirField = new TextField();
        exportDirChooseButton = new Button("选择目录");
        exportDirChooseButton.setOnAction(e -> chooseExportDirectoryV2());

        HBox exportDirBox = new HBox(8, exportDirField, exportDirChooseButton);
        HBox.setHgrow(exportDirField, Priority.ALWAYS);

        GridPane grid = new GridPane();
        grid.setHgap(14);
        grid.setVgap(10);
        addConfigItem(grid, 0, 0, "客户端编号", clientCodeField);
        addConfigItem(grid, 0, 1, "授权机构", authorizedOrgCombo);
        addConfigItem(grid, 1, 0, "监听协议", protocolTypeCombo);
        addConfigItem(grid, 1, 1, "数据导出目录", exportDirBox);
        addConfigItem(grid, 2, 0, "数据采集时间(秒)", syncIntervalField);
        addConfigItem(grid, 2, 1, "心跳时间(秒)", heartbeatIntervalField);

        protocolConfigBox = new VBox(8);

        configSaveButton = new Button("保存监听配置");
        configSaveButton.setOnAction(e -> saveMonitorConfig());
        configStatusLabel = new Label("监听配置使用应用默认值，可按协议覆盖保存。");
        configStatusLabel.setStyle("-fx-text-fill: #47617d;");

        HBox footer = new HBox(12, configSaveButton, configStatusLabel);
        footer.setAlignment(Pos.CENTER_LEFT);

        box.getChildren().addAll(title, grid, protocolConfigBox, footer, orgCodeField);
        return box;
    }

    private void addConfigItem(GridPane grid, int row, int block, String labelText, Node control) {
        int labelCol = block * 2;
        int controlCol = labelCol + 1;
        Label label = new Label(labelText);
        label.setStyle("-fx-text-fill: #35506b;");
        grid.add(label, labelCol, row);
        grid.add(control, controlCol, row);
        GridPane.setHgrow(control, Priority.ALWAYS);
    }

    private void refreshMonitorConfigUi() {
        populateAuthorizedOrgOptions();
        clientCodeField.setText(generatedClientCode);
        MonitorProtocolType protocol = protocolTypeCombo.getValue() == null ? MonitorProtocolType.HL7 : protocolTypeCombo.getValue();
        if (protocolTypeCombo.getItems().isEmpty()) {
            protocolTypeCombo.setItems(FXCollections.observableArrayList(MonitorProtocolType.values()));
        }
        protocolTypeCombo.setValue(protocol);
        applyProfileToForm(getOrCreateProfile(protocol));
        updateConfigEditStateV2();
    }

    private void populateAuthorizedOrgOptions() {
        if (authorizedOrgCombo == null) {
            return;
        }
        List<OrgInfo> options = new ArrayList<>(appState.getAuthorizedOrgInfos());
        if (options.isEmpty() && appState.getCurrentOrgInfo() != null) {
            options.add(appState.getCurrentOrgInfo());
        }
        authorizedOrgCombo.setItems(FXCollections.observableArrayList(options));
        String currentOrgCode = appState.getCurrentOrgCode();
        for (OrgInfo item : options) {
            if (item != null && valueOrDefault(item.getOrgCode(), "").equalsIgnoreCase(valueOrDefault(currentOrgCode, ""))) {
                authorizedOrgCombo.setValue(item);
                orgCodeField.setText(valueOrDefault(item.getOrgCode(), ""));
                return;
            }
        }
        if (!options.isEmpty()) {
            authorizedOrgCombo.setValue(options.get(0));
            orgCodeField.setText(valueOrDefault(options.get(0).getOrgCode(), ""));
        }
    }

    private MonitorListenerProfile getOrCreateProfile(MonitorProtocolType protocolType) {
        MonitorProtocolType protocol = protocolType == null ? MonitorProtocolType.HL7 : protocolType;
        MonitorListenerProfile cached = listenerProfileCache.get(protocol);
        if (cached != null) {
            return cached;
        }
        MonitorListenerProfile created = buildDefaultProfile(protocol);
        listenerProfileCache.put(protocol, created);
        return created;
    }

    private MonitorListenerProfile buildDefaultProfile(MonitorProtocolType protocolType) {
        MonitorListenerProfile profile = new MonitorListenerProfile();
        profile.setClientCode(generatedClientCode);
        profile.setMacAddress(localMacAddress);
        profile.setProtocolType(protocolType);
        profile.setOrgCode(appState.getCurrentOrgCode());
        profile.setOrgName(appState.getCurrentOrgInfo() == null ? null : appState.getCurrentOrgInfo().getOrgName());
        profile.setExportDir(valueOrDefault(config.get("exportDir"), "./exports"));
        profile.setSyncIntervalSec(parseInt(config.get("syncIntervalSec"), 60));
        profile.setHeartbeatIntervalSec(parseInt(config.get("heartbeatIntervalSec"), 60));
        profile.setListenPort(parseInt(config.get("hl7ListenerPort"), 5555));
        profile.setSerialPortName(valueOrDefault(config.get("serialPortName"), "COM1"));
        profile.setBaudRate(parseInt(config.get("serialBaudRate"), 9600));
        profile.setDataBits(parseInt(config.get("serialDataBits"), 8));
        profile.setStopBits(parseInt(config.get("serialStopBits"), 1));
        profile.setParity(valueOrDefault(config.get("serialParity"), "NONE"));
        profile.setReadTimeoutMs(parseInt(config.get("serialReadTimeoutMs"), 1000));
        profile.setPollIntervalMs(parseInt(config.get("serialPollIntervalMs"), 500));
        profile.setCharsetName(valueOrDefault(config.get("serialCharset"), "UTF-8"));
        profile.setFrameDelimiter(valueOrDefault(config.get("serialFrameDelimiter"), "CRLF"));
        profile.setStationNo(valueOrDefault(config.get("rs485StationNo"), "1"));
        return profile;
    }

    private void applyProfileToForm(MonitorListenerProfile profile) {
        if (profile == null) {
            return;
        }
        clientCodeField.setText(valueOrDefault(profile.getClientCode(), generatedClientCode));
        syncIntervalField.setText(String.valueOf(profile.getSyncIntervalSec()));
        heartbeatIntervalField.setText(String.valueOf(profile.getHeartbeatIntervalSec()));
        exportDirField.setText(valueOrDefault(profile.getExportDir(), ""));
        selectOrg(profile.getOrgCode());
        if (protocolTypeCombo.getValue() != profile.getProtocolType()) {
            protocolTypeCombo.setValue(profile.getProtocolType());
        }
        rebuildProtocolConfigFields(profile);
    }

    private void selectOrg(String orgCode) {
        if (authorizedOrgCombo == null || orgCode == null) {
            return;
        }
        for (OrgInfo item : authorizedOrgCombo.getItems()) {
            if (item != null && orgCode.equalsIgnoreCase(valueOrDefault(item.getOrgCode(), ""))) {
                authorizedOrgCombo.setValue(item);
                orgCodeField.setText(valueOrDefault(item.getOrgCode(), ""));
                return;
            }
        }
    }

    private void rebuildProtocolConfigFields(MonitorListenerProfile profile) {
        protocolConfigBox.getChildren().clear();
        GridPane grid = new GridPane();
        grid.setHgap(14);
        grid.setVgap(10);

        MonitorProtocolType protocolType = profile.getProtocolType();
        if (protocolType == MonitorProtocolType.HL7) {
            listenerPortField = new TextField(String.valueOf(profile.getListenPort() == null ? 5555 : profile.getListenPort()));
            readTimeoutField = new TextField(String.valueOf(profile.getReadTimeoutMs() == null ? 1000 : profile.getReadTimeoutMs()));
            charsetCombo = new ComboBox<>(FXCollections.observableArrayList("UTF-8", "GBK", "GB2312"));
            charsetCombo.setValue(valueOrDefault(profile.getCharsetName(), "UTF-8"));
            addConfigItem(grid, 0, 0, "监听端口", listenerPortField);
            addConfigItem(grid, 0, 1, "读取超时(ms)", readTimeoutField);
            addConfigItem(grid, 1, 0, "报文字符集", charsetCombo);
        } else {
            serialPortNameField = new TextField(valueOrDefault(profile.getSerialPortName(), "COM1"));
            baudRateCombo = new ComboBox<>(FXCollections.observableArrayList("1200", "2400", "4800", "9600", "19200", "38400", "57600", "115200"));
            baudRateCombo.setValue(String.valueOf(profile.getBaudRate() == null ? 9600 : profile.getBaudRate()));
            dataBitsCombo = new ComboBox<>(FXCollections.observableArrayList("5", "6", "7", "8"));
            dataBitsCombo.setValue(String.valueOf(profile.getDataBits() == null ? 8 : profile.getDataBits()));
            stopBitsCombo = new ComboBox<>(FXCollections.observableArrayList("1", "2"));
            stopBitsCombo.setValue(String.valueOf(profile.getStopBits() == null ? 1 : profile.getStopBits()));
            parityCombo = new ComboBox<>(FXCollections.observableArrayList("NONE", "ODD", "EVEN"));
            parityCombo.setValue(valueOrDefault(profile.getParity(), "NONE"));
            readTimeoutField = new TextField(String.valueOf(profile.getReadTimeoutMs() == null ? 1000 : profile.getReadTimeoutMs()));
            frameDelimiterCombo = new ComboBox<>(FXCollections.observableArrayList("CRLF", "CR", "LF"));
            frameDelimiterCombo.setValue(valueOrDefault(profile.getFrameDelimiter(), "CRLF"));
            charsetCombo = new ComboBox<>(FXCollections.observableArrayList("UTF-8", "GBK", "GB2312"));
            charsetCombo.setValue(valueOrDefault(profile.getCharsetName(), "UTF-8"));
            addConfigItem(grid, 0, 0, "串口号", serialPortNameField);
            addConfigItem(grid, 0, 1, "波特率", baudRateCombo);
            addConfigItem(grid, 1, 0, "数据位", dataBitsCombo);
            addConfigItem(grid, 1, 1, "停止位", stopBitsCombo);
            addConfigItem(grid, 2, 0, "校验位", parityCombo);
            addConfigItem(grid, 2, 1, "读取超时(ms)", readTimeoutField);
            addConfigItem(grid, 3, 0, "帧结束符", frameDelimiterCombo);
            addConfigItem(grid, 3, 1, "字符集", charsetCombo);
            if (protocolType == MonitorProtocolType.RS485) {
                stationNoField = new TextField(valueOrDefault(profile.getStationNo(), "1"));
                pollIntervalField = new TextField(String.valueOf(profile.getPollIntervalMs() == null ? 500 : profile.getPollIntervalMs()));
                addConfigItem(grid, 4, 0, "站号", stationNoField);
                addConfigItem(grid, 4, 1, "轮询间隔(ms)", pollIntervalField);
            }
        }
        protocolConfigBox.getChildren().add(grid);
    }

    private MonitorListenerProfile collectProfileFromForm() {
        if (protocolTypeCombo == null || protocolTypeCombo.getValue() == null) {
            return null;
        }
        MonitorListenerProfile profile = getOrCreateProfile(protocolTypeCombo.getValue());
        profile.setClientCode(generatedClientCode);
        profile.setMacAddress(localMacAddress);
        profile.setProtocolType(protocolTypeCombo.getValue());
        OrgInfo selectedOrg = authorizedOrgCombo == null ? null : authorizedOrgCombo.getValue();
        profile.setOrgCode(selectedOrg == null ? appState.getCurrentOrgCode() : selectedOrg.getOrgCode());
        profile.setOrgName(selectedOrg == null ? null : selectedOrg.getOrgName());
        profile.setExportDir(valueOrDefault(exportDirField == null ? null : exportDirField.getText(), ""));
        profile.setSyncIntervalSec(parseInt(syncIntervalField == null ? null : syncIntervalField.getText(), 60));
        profile.setHeartbeatIntervalSec(parseInt(heartbeatIntervalField == null ? null : heartbeatIntervalField.getText(), 60));
        if (profile.getProtocolType() == MonitorProtocolType.HL7) {
            profile.setListenPort(parseInt(listenerPortField == null ? null : listenerPortField.getText(), 5555));
            profile.setReadTimeoutMs(parseInt(readTimeoutField == null ? null : readTimeoutField.getText(), 1000));
            profile.setCharsetName(charsetCombo == null ? "UTF-8" : valueOrDefault(charsetCombo.getValue(), "UTF-8"));
        } else {
            profile.setSerialPortName(valueOrDefault(serialPortNameField == null ? null : serialPortNameField.getText(), "COM1"));
            profile.setBaudRate(parseInt(baudRateCombo == null ? null : baudRateCombo.getValue(), 9600));
            profile.setDataBits(parseInt(dataBitsCombo == null ? null : dataBitsCombo.getValue(), 8));
            profile.setStopBits(parseInt(stopBitsCombo == null ? null : stopBitsCombo.getValue(), 1));
            profile.setParity(valueOrDefault(parityCombo == null ? null : parityCombo.getValue(), "NONE"));
            profile.setReadTimeoutMs(parseInt(readTimeoutField == null ? null : readTimeoutField.getText(), 1000));
            profile.setFrameDelimiter(valueOrDefault(frameDelimiterCombo == null ? null : frameDelimiterCombo.getValue(), "CRLF"));
            profile.setCharsetName(valueOrDefault(charsetCombo == null ? null : charsetCombo.getValue(), "UTF-8"));
            profile.setPollIntervalMs(parseInt(pollIntervalField == null ? null : pollIntervalField.getText(), 500));
            profile.setStationNo(valueOrDefault(stationNoField == null ? null : stationNoField.getText(), "1"));
        }
        return profile;
    }

    private void updateConfigEditState() {
        boolean locked = isAnyListenerRunning();
        if (authorizedOrgCombo != null) {
            authorizedOrgCombo.setDisable(locked);
        }
        if (protocolTypeCombo != null) {
            protocolTypeCombo.setDisable(locked);
        }
        if (syncIntervalField != null) {
            syncIntervalField.setDisable(locked);
        }
        if (heartbeatIntervalField != null) {
            heartbeatIntervalField.setDisable(locked);
        }
        if (exportDirField != null) {
            exportDirField.setDisable(locked);
        }
        if (exportDirChooseButton != null) {
            exportDirChooseButton.setDisable(locked);
        }
        if (configSaveButton != null) {
            configSaveButton.setDisable(locked);
        }
        if (listenerPortField != null) {
            listenerPortField.setDisable(locked);
        }
        if (serialPortNameField != null) {
            serialPortNameField.setDisable(locked);
        }
        if (baudRateCombo != null) {
            baudRateCombo.setDisable(locked);
        }
        if (dataBitsCombo != null) {
            dataBitsCombo.setDisable(locked);
        }
        if (stopBitsCombo != null) {
            stopBitsCombo.setDisable(locked);
        }
        if (parityCombo != null) {
            parityCombo.setDisable(locked);
        }
        if (readTimeoutField != null) {
            readTimeoutField.setDisable(locked);
        }
        if (pollIntervalField != null) {
            pollIntervalField.setDisable(locked);
        }
        if (charsetCombo != null) {
            charsetCombo.setDisable(locked);
        }
        if (frameDelimiterCombo != null) {
            frameDelimiterCombo.setDisable(locked);
        }
        if (stationNoField != null) {
            stationNoField.setDisable(locked);
        }
        if (configStatusLabel != null) {
            configStatusLabel.setText(locked ? "监听已启动，当前机构和协议参数已锁定。" : "监听未启动，可修改并保存当前协议配置。");
        }
    }

    private void chooseExportDirectory() {
        DirectoryChooser chooser = new DirectoryChooser();
        if (exportDirField != null && exportDirField.getText() != null && !exportDirField.getText().isBlank()) {
            Path path = Paths.get(exportDirField.getText());
            if (path.toFile().exists()) {
                chooser.setInitialDirectory(path.toFile());
            }
        }
        chooser.setTitle("选择数据导出目录");
        java.io.File selected = chooser.showDialog(primaryStage);
        if (selected != null && exportDirField != null) {
            exportDirField.setText(selected.getAbsolutePath());
        }
    }

    private void loadRemoteProfilesAsync() {
        ioExecutor.submit(() -> {
            try {
                List<MonitorListenerProfile> items = monitorConfigApiClient.listProfiles(appState.getToken(), localMacAddress);
                Platform.runLater(() -> {
                    for (MonitorListenerProfile item : items) {
                        if (item != null) {
                            listenerProfileCache.put(item.getProtocolType(), item);
                        }
                    }
                    applyProfileToForm(getOrCreateProfile(protocolTypeCombo == null ? MonitorProtocolType.HL7 : protocolTypeCombo.getValue()));
                    if (configStatusLabel != null) {
                        configStatusLabel.setText(items.isEmpty() ? "未读取到服务端监听配置，当前使用默认配置。" : "已读取服务端监听配置。");
                    }
                    updateConfigEditState();
                });
            } catch (Exception ex) {
                appendLog("读取服务端监听配置失败: " + ex.getMessage());
                Platform.runLater(() -> {
                    if (configStatusLabel != null) {
                        configStatusLabel.setText("读取服务端监听配置失败，当前使用本地默认配置。");
                    }
                });
            }
        });
    }

    private GridPane buildConfigBoxCn() {
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(8);
        int row = 0;
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
        stopMonitorListeners();
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

    private void saveMonitorConfig() {
        MonitorListenerProfile profile = collectProfileFromForm();
        if (profile == null) {
            showAlert(Alert.AlertType.ERROR, "保存失败", "请先选择监听协议。");
            return;
        }
        listenerProfileCache.put(profile.getProtocolType(), cloneProfile(profile));
        persistProfileToLocalConfig(profile);
        try {
            config.saveExternal();
        } catch (Exception ex) {
            appendLog("本地监听配置保存失败: " + ex.getMessage());
            showAlert(Alert.AlertType.ERROR, "保存失败", ex.getMessage());
            return;
        }
        if (configStatusLabel != null) {
            configStatusLabel.setText("正在保存监听配置...");
        }
        ioExecutor.submit(() -> {
            try {
                MonitorListenerProfile saved = monitorConfigApiClient.saveProfile(appState.getToken(), profile);
                MonitorListenerProfile finalProfile = saved == null ? profile : saved;
                listenerProfileCache.put(finalProfile.getProtocolType(), cloneProfile(finalProfile));
                appendLog("监听配置已保存到服务端: protocol=" + finalProfile.getProtocolType().getCode()
                    + ", mac=" + valueOrDefault(finalProfile.getMacAddress(), "-"));
                store.saveRuntimeEvent("CONFIG", "INFO", "ui", "Monitor config saved", null);
                Platform.runLater(() -> {
                    applyProfileToForm(finalProfile);
                    if (configStatusLabel != null) {
                        configStatusLabel.setText("监听配置已保存。");
                    }
                    updateConfigEditStateV2();
                });
            } catch (Exception ex) {
                appendLog("服务端监听配置保存失败: " + ex.getMessage());
                store.saveRuntimeEvent("CONFIG", "ERROR", "ui", ex.getMessage(), null);
                Platform.runLater(() -> {
                    if (configStatusLabel != null) {
                        configStatusLabel.setText("服务端监听配置保存失败。");
                    }
                    showAlert(Alert.AlertType.ERROR, "保存失败", valueOrDefault(ex.getMessage(), "服务端监听配置保存失败"));
                });
            }
        });
    }

    private void startMonitorListeners() {
        ioExecutor.submit(() -> {
            MonitorListenerProfile profile = collectProfileFromForm();
            if (profile == null) {
                Platform.runLater(() -> showAlert(Alert.AlertType.ERROR, "启动失败", "请先配置监听协议。"));
                return;
            }
            listenerProfileCache.put(profile.getProtocolType(), cloneProfile(profile));
            persistProfileToLocalConfig(profile);
            try {
                config.saveExternal();
            } catch (Exception ignored) {
            }

            if (profile.getProtocolType() == MonitorProtocolType.HL7) {
                stopSerialListenerQuietly();
                LinkedHashSet<Integer> ports = new LinkedHashSet<>(repository.listActiveListenPorts(
                    appState.getCurrentOrgCode(),
                    profile.getListenPort() == null ? 5555 : profile.getListenPort()
                ));
                if (profile.getListenPort() != null && profile.getListenPort() > 0) {
                    ports.add(profile.getListenPort());
                }
                listenerManager.start(new ArrayList<>(ports));
                store.saveRuntimeEvent("MLLP", "INFO", "listener", "Listeners started " + ports, null);
                appendLog("HL7 监听已启动: " + ports);
            } else {
                if (listenerManager != null) {
                    listenerManager.stop();
                }
                stopSerialListenerQuietly();
                serialProtocolListener = new SerialProtocolListener(profile, logger, this::handleSerialProtocolMessage);
                serialProtocolListener.start();
                store.saveRuntimeEvent(profile.getProtocolType().getCode(), "INFO", "listener",
                    "Serial listener started " + valueOrDefault(profile.getSerialPortName(), "-"), null);
                appendLog(profile.getProtocolType().getDisplayName() + "监听已启动: "
                    + valueOrDefault(profile.getSerialPortName(), "-"));
            }
            Platform.runLater(() -> {
                if (configStatusLabel != null) {
                    configStatusLabel.setText(profile.getProtocolType().getDisplayName() + "监听已启动。");
                }
                refreshStatusLabels();
                updateConfigEditStateV2();
            });
        });
    }

    private void stopMonitorListeners() {
        if (listenerManager != null) {
            listenerManager.stop();
            store.saveRuntimeEvent("MLLP", "INFO", "listener", "Listeners stopped", null);
        }
        stopSerialListenerQuietly();
        Platform.runLater(() -> {
            if (configStatusLabel != null) {
                configStatusLabel.setText("监听已停止，可以调整当前配置。");
            }
            refreshStatusLabels();
            updateConfigEditStateV2();
        });
    }

    private boolean isAnyListenerRunning() {
        return (listenerManager != null && listenerManager.isRunning())
            || (serialProtocolListener != null && serialProtocolListener.isRunning());
    }

    private void stopSerialListenerQuietly() {
        if (serialProtocolListener == null) {
            return;
        }
        try {
            serialProtocolListener.stop();
        } catch (Exception ignored) {
        } finally {
            serialProtocolListener = null;
        }
    }

    private void updateConfigEditStateV2() {
        boolean locked = isAnyListenerRunning();
        if (authorizedOrgCombo != null) {
            authorizedOrgCombo.setDisable(locked);
        }
        if (protocolTypeCombo != null) {
            protocolTypeCombo.setDisable(locked);
        }
        if (syncIntervalField != null) {
            syncIntervalField.setDisable(locked);
        }
        if (heartbeatIntervalField != null) {
            heartbeatIntervalField.setDisable(locked);
        }
        if (exportDirField != null) {
            exportDirField.setDisable(locked);
        }
        if (exportDirChooseButton != null) {
            exportDirChooseButton.setDisable(locked);
        }
        if (configSaveButton != null) {
            configSaveButton.setDisable(locked);
        }
        if (listenerPortField != null) {
            listenerPortField.setDisable(locked);
        }
        if (serialPortNameField != null) {
            serialPortNameField.setDisable(locked);
        }
        if (baudRateCombo != null) {
            baudRateCombo.setDisable(locked);
        }
        if (dataBitsCombo != null) {
            dataBitsCombo.setDisable(locked);
        }
        if (stopBitsCombo != null) {
            stopBitsCombo.setDisable(locked);
        }
        if (parityCombo != null) {
            parityCombo.setDisable(locked);
        }
        if (readTimeoutField != null) {
            readTimeoutField.setDisable(locked);
        }
        if (pollIntervalField != null) {
            pollIntervalField.setDisable(locked);
        }
        if (charsetCombo != null) {
            charsetCombo.setDisable(locked);
        }
        if (frameDelimiterCombo != null) {
            frameDelimiterCombo.setDisable(locked);
        }
        if (stationNoField != null) {
            stationNoField.setDisable(locked);
        }
        if (configStatusLabel != null && configStatusLabel.getText() != null && configStatusLabel.getText().isBlank()) {
            configStatusLabel.setText(locked ? "监听已启动，当前参数已锁定。" : "请先保存当前监听配置。");
        }
    }

    private void chooseExportDirectoryV2() {
        DirectoryChooser chooser = new DirectoryChooser();
        if (exportDirField != null && exportDirField.getText() != null && !exportDirField.getText().isBlank()) {
            Path path = Paths.get(exportDirField.getText());
            if (Files.exists(path) && Files.isDirectory(path)) {
                chooser.setInitialDirectory(path.toFile());
            }
        }
        chooser.setTitle("选择数据导出目录");
        java.io.File selected = chooser.showDialog(primaryStage);
        if (selected != null && exportDirField != null) {
            exportDirField.setText(selected.getAbsolutePath());
        }
    }

    private void loadRemoteProfilesAsyncV2() {
        ioExecutor.submit(() -> {
            try {
                List<MonitorListenerProfile> items = monitorConfigApiClient.listProfiles(appState.getToken(), localMacAddress);
                Platform.runLater(() -> {
                    for (MonitorListenerProfile item : items) {
                        if (item != null) {
                            listenerProfileCache.put(item.getProtocolType(), cloneProfile(item));
                        }
                    }
                    applyProfileToForm(getOrCreateProfile(protocolTypeCombo == null ? MonitorProtocolType.HL7 : protocolTypeCombo.getValue()));
                    if (configStatusLabel != null) {
                        configStatusLabel.setText(items.isEmpty()
                            ? "未读取到服务端监听配置，当前使用默认配置。"
                            : "已读取服务端监听配置。");
                    }
                    updateConfigEditStateV2();
                });
            } catch (Exception ex) {
                appendLog("读取服务端监听配置失败: " + ex.getMessage());
                Platform.runLater(() -> {
                    if (configStatusLabel != null) {
                        configStatusLabel.setText("读取服务端监听配置失败，当前使用本地默认配置。");
                    }
                });
            }
        });
    }

    private void handleSerialProtocolMessage(MonitorProtocolType protocolType, String portName, String payload) {
        if (payload == null || payload.isBlank()) {
            return;
        }
        String text = payload.trim();
        if (text.startsWith("MSH")) {
            handleHl7Message(new MllpServer.MessageContext(
                0,
                protocolType.getCode() + ":" + valueOrDefault(portName, "-"),
                valueOrDefault(portName, protocolType.getCode()),
                0,
                text
            ));
            return;
        }
        appendLog(protocolType.getDisplayName() + "收到原始数据: " + text);
        store.saveRuntimeEvent(protocolType.getCode(), "INFO", valueOrDefault(portName, "serial"), text, null);
    }

    private void persistProfileToLocalConfig(MonitorListenerProfile profile) {
        if (profile == null) {
            return;
        }
        config.set("clientCode", valueOrDefault(profile.getClientCode(), generatedClientCode));
        config.set("orgCode", valueOrDefault(profile.getOrgCode(), appState.getCurrentOrgCode()));
        config.set("listenerProtocol", profile.getProtocolType().getCode());
        config.set("exportDir", valueOrDefault(profile.getExportDir(), "./exports"));
        config.set("syncIntervalSec", String.valueOf(profile.getSyncIntervalSec()));
        config.set("heartbeatIntervalSec", String.valueOf(profile.getHeartbeatIntervalSec()));
        if (profile.getListenPort() != null) {
            config.set("hl7ListenerPort", String.valueOf(profile.getListenPort()));
        }
        config.set("serialPortName", valueOrDefault(profile.getSerialPortName(), "COM1"));
        config.set("serialBaudRate", String.valueOf(profile.getBaudRate() == null ? 9600 : profile.getBaudRate()));
        config.set("serialDataBits", String.valueOf(profile.getDataBits() == null ? 8 : profile.getDataBits()));
        config.set("serialStopBits", String.valueOf(profile.getStopBits() == null ? 1 : profile.getStopBits()));
        config.set("serialParity", valueOrDefault(profile.getParity(), "NONE"));
        config.set("serialReadTimeoutMs", String.valueOf(profile.getReadTimeoutMs() == null ? 1000 : profile.getReadTimeoutMs()));
        config.set("serialPollIntervalMs", String.valueOf(profile.getPollIntervalMs() == null ? 500 : profile.getPollIntervalMs()));
        config.set("serialCharset", valueOrDefault(profile.getCharsetName(), "UTF-8"));
        config.set("serialFrameDelimiter", valueOrDefault(profile.getFrameDelimiter(), "CRLF"));
        config.set("rs485StationNo", valueOrDefault(profile.getStationNo(), "1"));
        if (orgCodeField != null) {
            orgCodeField.setText(valueOrDefault(profile.getOrgCode(), ""));
        }
        if (profile.getOrgCode() != null) {
            appState.selectAuthorizedOrg(profile.getOrgCode());
        }
    }

    private MonitorListenerProfile cloneProfile(MonitorListenerProfile source) {
        if (source == null) {
            return null;
        }
        MonitorListenerProfile target = new MonitorListenerProfile();
        target.setId(source.getId());
        target.setClientCode(source.getClientCode());
        target.setMacAddress(source.getMacAddress());
        target.setProtocolType(source.getProtocolType());
        target.setOrgCode(source.getOrgCode());
        target.setOrgName(source.getOrgName());
        target.setExportDir(source.getExportDir());
        target.setSyncIntervalSec(source.getSyncIntervalSec());
        target.setHeartbeatIntervalSec(source.getHeartbeatIntervalSec());
        target.setListenPort(source.getListenPort());
        target.setSerialPortName(source.getSerialPortName());
        target.setBaudRate(source.getBaudRate());
        target.setDataBits(source.getDataBits());
        target.setStopBits(source.getStopBits());
        target.setParity(source.getParity());
        target.setReadTimeoutMs(source.getReadTimeoutMs());
        target.setPollIntervalMs(source.getPollIntervalMs());
        target.setCharsetName(source.getCharsetName());
        target.setFrameDelimiter(source.getFrameDelimiter());
        target.setStationNo(source.getStationNo());
        target.setExtJson(source.getExtJson());
        return target;
    }

    private void showAboutDialog() {
        OrgInfo orgInfo = appState == null ? null : appState.getCurrentOrgInfo();
        String orgDisplay = appState == null
            ? "-"
            : joinText(appState.getCurrentOrgCode(), orgInfo == null ? null : orgInfo.getOrgName(), " / ");
        String content = "系统名称: 寅米医疗器械接入终端软件"
            + System.lineSeparator() + "当前版本: " + appState.getAppVersion()
            + System.lineSeparator() + "应用编码: " + valueOrDefault(config.get("appUpgradeCode"), "InMeHL7MonitorClient")
            + System.lineSeparator() + "客户端编号: " + valueOrDefault(generatedClientCode, "-")
            + System.lineSeparator() + "本机 MAC: " + valueOrDefault(localMacAddress, "-")
            + System.lineSeparator() + "授权机构: " + valueOrDefault(orgDisplay, "-");
        showAlert(Alert.AlertType.INFORMATION, "关于", content);
    }

    private void checkForUpdateAsync() {
        if (configStatusLabel != null) {
            configStatusLabel.setText("正在检查最新版本...");
        }
        ioExecutor.submit(() -> {
            try {
                VersionInfo info = fetchLatestVersionInfo();
                Platform.runLater(() -> handleVersionInfo(info));
            } catch (Exception ex) {
                appendLog("检查版本升级失败: " + ex.getMessage());
                Platform.runLater(() -> {
                    if (configStatusLabel != null) {
                        configStatusLabel.setText("检查版本升级失败。");
                    }
                    showAlert(Alert.AlertType.ERROR, "检查升级失败", valueOrDefault(ex.getMessage(), "无法获取版本信息"));
                });
            }
        });
    }

    private VersionInfo fetchLatestVersionInfo() throws IOException, InterruptedException {
        String baseUrl = valueOrDefault(config.get("appBaseUrl"), "https://hisplus.zhunyintech.com");
        String appCode = valueOrDefault(config.get("appUpgradeCode"), "InMeHL7MonitorClient");
        String url = trimTrailingSlash(baseUrl) + "/api/zyhisplus/v1/appversion/latest/" + appCode;
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(15))
            .header("accept", "application/json, */*")
            .GET();
        applyTokenHeaders(builder, appState == null ? null : appState.getToken());
        appendLog("HTTP GET " + url);
        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        appendLog("HTTP GET " + url + " -> " + response.statusCode());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("GET " + url + " -> HTTP " + response.statusCode() + " " + response.body());
        }
        JsonNode root = mapper.readTree(response.body());
        JsonNode codeNode = root.get("code");
        if (codeNode != null && codeNode.asInt(0) != 0) {
            throw new IOException(valueOrDefault(text(root, "message"), valueOrDefault(text(root, "errMessage"), "无法获取版本信息")));
        }
        JsonNode node = unwrapVersionNode(root.has("data") ? root.path("data") : root);
        if (node == null || node.isNull() || node.isMissingNode()) {
            return new VersionInfo(appState.getAppVersion(), null, false, null);
        }
        return new VersionInfo(
            valueOrDefault(text(node, "version"), appState.getAppVersion()),
            text(node, "fileUrl"),
            node.path("forceUpdate").asBoolean(false),
            valueOrDefault(text(node, "description"), text(node, "remark"))
        );
    }

    private void handleVersionInfo(VersionInfo info) {
        String currentVersion = appState.getAppVersion();
        if (info == null || valueOrDefault(info.version(), "").isBlank() || currentVersion.equals(info.version())) {
            if (configStatusLabel != null) {
                configStatusLabel.setText("当前已是最新版本。");
            }
            showAlert(Alert.AlertType.INFORMATION, "版本升级", "当前版本为 " + currentVersion + "，暂无新版本。");
            return;
        }
        StringBuilder content = new StringBuilder();
        content.append("当前版本: ").append(currentVersion).append(System.lineSeparator());
        content.append("最新版本: ").append(info.version()).append(System.lineSeparator());
        if (!valueOrDefault(info.description(), "").isBlank()) {
            content.append("更新说明: ").append(info.description()).append(System.lineSeparator());
        }
        content.append(System.lineSeparator()).append("是否立即下载并升级？");
        Alert alert = new Alert(info.forceUpdate() ? Alert.AlertType.WARNING : Alert.AlertType.CONFIRMATION);
        alert.setTitle("版本升级");
        alert.setHeaderText("发现新版本 " + info.version());
        alert.setContentText(content.toString());
        if (primaryStage != null) {
            alert.initOwner(primaryStage);
        }
        if (alert.showAndWait().orElse(ButtonType.CANCEL) == ButtonType.OK) {
            downloadUpdateAsync(info);
        } else if (configStatusLabel != null) {
            configStatusLabel.setText("已取消版本升级。");
        }
    }

    private void downloadUpdateAsync(VersionInfo info) {
        if (info == null || valueOrDefault(info.fileUrl(), "").isBlank()) {
            showAlert(Alert.AlertType.ERROR, "版本升级", "当前版本未配置下载地址。");
            return;
        }
        if (configStatusLabel != null) {
            configStatusLabel.setText("正在下载升级包...");
        }
        ioExecutor.submit(() -> {
            try {
                Path target = downloadUpdatePackage(info.fileUrl());
                appendLog("升级包已下载: " + target);
                Platform.runLater(() -> {
                    if (configStatusLabel != null) {
                        configStatusLabel.setText("升级包已下载完成。");
                    }
                    if (isInstaller(target)) {
                        if (launchInstaller(target)) {
                            showAlert(Alert.AlertType.INFORMATION, "版本升级", "安装程序已启动，请按提示完成升级。");
                            Platform.exit();
                        } else {
                            showAlert(Alert.AlertType.ERROR, "版本升级", "升级包已下载，但无法自动启动安装程序。");
                            openExportLocation(target);
                        }
                    } else {
                        showAlert(Alert.AlertType.INFORMATION, "版本升级",
                            "升级包已下载到:\n" + target.toAbsolutePath() + "\n请在安装或替换完成后重新启动客户端。");
                        openExportLocation(target);
                    }
                });
            } catch (Exception ex) {
                appendLog("下载升级包失败: " + ex.getMessage());
                Platform.runLater(() -> {
                    if (configStatusLabel != null) {
                        configStatusLabel.setText("下载升级包失败。");
                    }
                    showAlert(Alert.AlertType.ERROR, "版本升级", valueOrDefault(ex.getMessage(), "下载升级包失败"));
                });
            }
        });
    }

    private Path downloadUpdatePackage(String fileUrl) throws IOException, InterruptedException {
        Path target = resolveUpdateFileTarget(fileUrl);
        Files.createDirectories(target.getParent());
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(fileUrl))
            .timeout(Duration.ofMinutes(5))
            .GET()
            .build();
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("GET " + fileUrl + " -> HTTP " + response.statusCode());
        }
        Files.write(target, response.body());
        return target;
    }

    private Path resolveUpdateFileTarget(String fileUrl) {
        String local = System.getenv("LOCALAPPDATA");
        if (local == null || local.isBlank()) {
            local = System.getProperty("user.home");
        }
        return Paths.get(local, "InMeHL7MonitorClient", "updates", resolveFileName(fileUrl));
    }

    private String resolveFileName(String fileUrl) {
        if (fileUrl == null || fileUrl.isBlank()) {
            return "InMeHL7MonitorClient-Update.bin";
        }
        String clean = fileUrl;
        int index = clean.indexOf('?');
        if (index >= 0) {
            clean = clean.substring(0, index);
        }
        int slash = clean.lastIndexOf('/');
        String name = slash >= 0 ? clean.substring(slash + 1) : clean;
        return name == null || name.isBlank() ? "InMeHL7MonitorClient-Update.bin" : name;
    }

    private boolean isInstaller(Path target) {
        if (target == null) {
            return false;
        }
        String lower = target.getFileName().toString().toLowerCase();
        return lower.endsWith(".exe") || lower.endsWith(".msi");
    }

    private boolean launchInstaller(Path installer) {
        try {
            new ProcessBuilder("cmd", "/c", "start", "\"\"", installer.toAbsolutePath().toString()).start();
            return true;
        } catch (Exception ex) {
            appendLog("启动安装程序失败: " + ex.getMessage());
            return false;
        }
    }

    private JsonNode unwrapVersionNode(JsonNode node) {
        JsonNode current = node;
        for (int i = 0; i < 3; i++) {
            if (current == null || current.isNull() || current.isMissingNode()) {
                return current;
            }
            if (current.has("version")) {
                return current;
            }
            if (current.isArray()) {
                return current.size() > 0 ? current.get(0) : current;
            }
            JsonNode records = current.path("records");
            if (records.isArray() && records.size() > 0) {
                return records.get(0);
            }
            JsonNode content = current.path("content");
            if (content.isArray() && content.size() > 0) {
                return content.get(0);
            }
            JsonNode next = current.path("data");
            if (next.isMissingNode() || next.isNull() || next == current) {
                return current;
            }
            current = next;
        }
        return current;
    }

    private void applyTokenHeaders(HttpRequest.Builder builder, String token) {
        if (builder == null || token == null || token.isBlank()) {
            return;
        }
        String tokenValue = token.trim();
        String headerName = valueOrDefault(config.get("authHeaderName"), "x-auth-token");
        String mode = valueOrDefault(config.get("authHeaderMode"), "auto").trim().toLowerCase();
        if ("auto".equals(mode)) {
            mode = looksLikeJwt(tokenValue) ? "authorization" : "x-auth-token";
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

    private boolean looksLikeJwt(String tokenValue) {
        int firstDot = tokenValue.indexOf('.');
        return firstDot > 0 && tokenValue.indexOf('.', firstDot + 1) > firstDot + 1;
    }

    private String trimTrailingSlash(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String text = value.trim();
        return text.endsWith("/") ? text.substring(0, text.length() - 1) : text;
    }

    private String text(JsonNode node, String field) {
        if (node == null || field == null || node.get(field) == null || node.get(field).isNull()) {
            return null;
        }
        String value = node.get(field).asText();
        return value == null || value.isBlank() ? null : value.trim();
    }

    private void showAlert(Alert.AlertType type, String title, String message) {
        Alert alert = new Alert(type);
        alert.setTitle(title);
        alert.setHeaderText(null);
        alert.setContentText(valueOrDefault(message, ""));
        if (primaryStage != null) {
            alert.initOwner(primaryStage);
        }
        alert.showAndWait();
    }

    private void saveConfig() {
        config.set("ssoBaseUrl", valueFromFieldOrConfig(ssoBaseUrlField, "ssoBaseUrl"));
        config.set("ssoAppId", valueFromFieldOrConfig(ssoAppIdField, "ssoAppId"));
        config.set("appBaseUrl", valueFromFieldOrConfig(appBaseUrlField, "appBaseUrl"));
        config.set("serverClientApiBase", valueFromFieldOrConfig(serverApiBaseField, "serverClientApiBase"));
        config.set("clientCode", valueFromFieldOrConfig(clientCodeField, "clientCode"));
        config.set("orgCode", valueFromFieldOrConfig(orgCodeField, "orgCode"));
        config.set("deviceId", valueFromFieldOrConfig(deviceIdField, "deviceId"));
        config.set("hl7ListenerPort", valueFromFieldOrConfig(listenerPortField, "hl7ListenerPort"));
        config.set("syncIntervalSec", valueFromFieldOrConfig(syncIntervalField, "syncIntervalSec"));
        config.set("heartbeatIntervalSec", valueFromFieldOrConfig(heartbeatIntervalField, "heartbeatIntervalSec"));
        config.set("exportDir", valueFromFieldOrConfig(exportDirField, "exportDir"));
        try {
            config.saveExternal();
            appendLog("配置已保存");
            store.saveRuntimeEvent("CONFIG", "INFO", "ui", "Config saved", null);
        } catch (Exception ex) {
            appendLog("配置保存失败: " + ex.getMessage());
            store.saveRuntimeEvent("CONFIG", "ERROR", "ui", ex.getMessage(), null);
        }
    }

    private String valueFromFieldOrConfig(TextField field, String key) {
        if (field == null) {
            return valueOrDefault(config.get(key), "");
        }
        return field.getText();
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
            if (isAnyListenerRunning()) {
                startMonitorListeners();
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

    private record VersionInfo(String version, String fileUrl, boolean forceUpdate, String description) {
    }

    public static void main(String[] args) {
        launch(args);
    }
}
