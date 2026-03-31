package com.zhunyintech.inmehl7.client.model;

import java.time.LocalDateTime;

public class OrgInfo {
    private String orgCode;
    private String orgName;
    private String hisCode;
    private String siOrgCode;
    private String webserviceUrl;
    private String ssoAccount;
    private String operId;
    private String realName;
    private String tokenSnapshot;
    private LocalDateTime lastLoginAt;

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

    public String getHisCode() {
        return hisCode;
    }

    public void setHisCode(String hisCode) {
        this.hisCode = hisCode;
    }

    public String getSiOrgCode() {
        return siOrgCode;
    }

    public void setSiOrgCode(String siOrgCode) {
        this.siOrgCode = siOrgCode;
    }

    public String getWebserviceUrl() {
        return webserviceUrl;
    }

    public void setWebserviceUrl(String webserviceUrl) {
        this.webserviceUrl = webserviceUrl;
    }

    public String getSsoAccount() {
        return ssoAccount;
    }

    public void setSsoAccount(String ssoAccount) {
        this.ssoAccount = ssoAccount;
    }

    public String getOperId() {
        return operId;
    }

    public void setOperId(String operId) {
        this.operId = operId;
    }

    public String getRealName() {
        return realName;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public String getTokenSnapshot() {
        return tokenSnapshot;
    }

    public void setTokenSnapshot(String tokenSnapshot) {
        this.tokenSnapshot = tokenSnapshot;
    }

    public LocalDateTime getLastLoginAt() {
        return lastLoginAt;
    }

    public void setLastLoginAt(LocalDateTime lastLoginAt) {
        this.lastLoginAt = lastLoginAt;
    }
}
