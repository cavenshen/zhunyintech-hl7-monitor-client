package com.zhunyintech.inmehl7.client.support;

import java.net.NetworkInterface;
import java.util.Enumeration;

public final class ClientMachineSupport {

    private ClientMachineSupport() {
    }

    public static String resolveLocalMacAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface net = interfaces.nextElement();
                if (net == null || net.isLoopback() || !net.isUp()) {
                    continue;
                }
                byte[] mac = net.getHardwareAddress();
                if (mac == null || mac.length == 0) {
                    continue;
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < mac.length; i++) {
                    sb.append(String.format("%02X", mac[i]));
                    if (i < mac.length - 1) {
                        sb.append("-");
                    }
                }
                return sb.toString();
            }
        } catch (Exception ignored) {
        }
        return "UNKNOWN";
    }

    public static String buildClientCode(String macAddress) {
        String compact = macAddress == null ? "UNKNOWN" : macAddress.replace("-", "").replace(":", "");
        if (compact.length() > 8) {
            compact = compact.substring(compact.length() - 8);
        }
        return "inmehl7-" + compact.toLowerCase();
    }
}
