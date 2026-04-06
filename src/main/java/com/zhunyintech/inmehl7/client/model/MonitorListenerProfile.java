package com.zhunyintech.inmehl7.client.model;

public class MonitorListenerProfile {
    private Long id;
    private String clientCode;
    private String macAddress;
    private MonitorProtocolType protocolType = MonitorProtocolType.HL7;
    private String orgCode;
    private String orgName;
    private String exportDir;
    private int syncIntervalSec = 60;
    private int heartbeatIntervalSec = 60;
    private Integer listenPort = 5555;
    private String serialPortName;
    private Integer baudRate = 9600;
    private Integer dataBits = 8;
    private Integer stopBits = 1;
    private String parity = "NONE";
    private Integer readTimeoutMs = 1000;
    private Integer pollIntervalMs = 500;
    private String charsetName = "UTF-8";
    private String frameDelimiter = "CRLF";
    private String stationNo = "1";
    private String extJson;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getClientCode() {
        return clientCode;
    }

    public void setClientCode(String clientCode) {
        this.clientCode = clientCode;
    }

    public String getMacAddress() {
        return macAddress;
    }

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
    }

    public MonitorProtocolType getProtocolType() {
        return protocolType;
    }

    public void setProtocolType(MonitorProtocolType protocolType) {
        this.protocolType = protocolType == null ? MonitorProtocolType.HL7 : protocolType;
    }

    public String getOrgCode() {
        return orgCode;
    }

    public void setOrgCode(String orgCode) {
        this.orgCode = orgCode;
    }

    public String getOrgName() {
        return orgName;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public String getExportDir() {
        return exportDir;
    }

    public void setExportDir(String exportDir) {
        this.exportDir = exportDir;
    }

    public int getSyncIntervalSec() {
        return syncIntervalSec;
    }

    public void setSyncIntervalSec(int syncIntervalSec) {
        this.syncIntervalSec = syncIntervalSec;
    }

    public int getHeartbeatIntervalSec() {
        return heartbeatIntervalSec;
    }

    public void setHeartbeatIntervalSec(int heartbeatIntervalSec) {
        this.heartbeatIntervalSec = heartbeatIntervalSec;
    }

    public Integer getListenPort() {
        return listenPort;
    }

    public void setListenPort(Integer listenPort) {
        this.listenPort = listenPort;
    }

    public String getSerialPortName() {
        return serialPortName;
    }

    public void setSerialPortName(String serialPortName) {
        this.serialPortName = serialPortName;
    }

    public Integer getBaudRate() {
        return baudRate;
    }

    public void setBaudRate(Integer baudRate) {
        this.baudRate = baudRate;
    }

    public Integer getDataBits() {
        return dataBits;
    }

    public void setDataBits(Integer dataBits) {
        this.dataBits = dataBits;
    }

    public Integer getStopBits() {
        return stopBits;
    }

    public void setStopBits(Integer stopBits) {
        this.stopBits = stopBits;
    }

    public String getParity() {
        return parity;
    }

    public void setParity(String parity) {
        this.parity = parity;
    }

    public Integer getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(Integer readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    public Integer getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(Integer pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    public String getFrameDelimiter() {
        return frameDelimiter;
    }

    public void setFrameDelimiter(String frameDelimiter) {
        this.frameDelimiter = frameDelimiter;
    }

    public String getStationNo() {
        return stationNo;
    }

    public void setStationNo(String stationNo) {
        this.stationNo = stationNo;
    }

    public String getExtJson() {
        return extJson;
    }

    public void setExtJson(String extJson) {
        this.extJson = extJson;
    }
}
