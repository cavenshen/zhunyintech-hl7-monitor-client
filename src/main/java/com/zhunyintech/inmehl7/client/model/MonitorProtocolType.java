package com.zhunyintech.inmehl7.client.model;

public enum MonitorProtocolType {
    HL7("HL7", "HL7协议"),
    RS232("RS232", "RS232协议"),
    RS485("RS485", "RS485协议");

    private final String code;
    private final String displayName;

    MonitorProtocolType(String code, String displayName) {
        this.code = code;
        this.displayName = displayName;
    }

    public String getCode() {
        return code;
    }

    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }

    public static MonitorProtocolType of(String value) {
        if (value != null) {
            for (MonitorProtocolType item : values()) {
                if (item.code.equalsIgnoreCase(value) || item.displayName.equalsIgnoreCase(value)) {
                    return item;
                }
            }
        }
        return HL7;
    }
}
