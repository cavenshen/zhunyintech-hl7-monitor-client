package com.zhunyintech.inmehl7.client.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Hl7ResultRecord {
    private String recordId;
    private String messageType;
    private String triggerEvent;
    private String hl7Version;
    private String sourceApplication;
    private String sourceFacility;
    private String messageControlId;
    private String sampleNo;
    private String patientId;
    private String patientName;
    private String patientGender;
    private String patientAge;
    private String wardName;
    private String bedName;
    private LocalDateTime resultTime;
    private String resultType = "ORU_R01";
    private String dataCategory = "VITAL_SIGN";
    private String hisTarget = "inme-his";
    private String hisRequestNo;
    private String rawMessage;
    private List<Hl7Observation> observations = new ArrayList<>();

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getTriggerEvent() {
        return triggerEvent;
    }

    public void setTriggerEvent(String triggerEvent) {
        this.triggerEvent = triggerEvent;
    }

    public String getHl7Version() {
        return hl7Version;
    }

    public void setHl7Version(String hl7Version) {
        this.hl7Version = hl7Version;
    }

    public String getSourceApplication() {
        return sourceApplication;
    }

    public void setSourceApplication(String sourceApplication) {
        this.sourceApplication = sourceApplication;
    }

    public String getSourceFacility() {
        return sourceFacility;
    }

    public void setSourceFacility(String sourceFacility) {
        this.sourceFacility = sourceFacility;
    }

    public String getMessageControlId() {
        return messageControlId;
    }

    public void setMessageControlId(String messageControlId) {
        this.messageControlId = messageControlId;
    }

    public String getSampleNo() {
        return sampleNo;
    }

    public void setSampleNo(String sampleNo) {
        this.sampleNo = sampleNo;
    }

    public String getPatientId() {
        return patientId;
    }

    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    public String getPatientName() {
        return patientName;
    }

    public void setPatientName(String patientName) {
        this.patientName = patientName;
    }

    public String getPatientGender() {
        return patientGender;
    }

    public void setPatientGender(String patientGender) {
        this.patientGender = patientGender;
    }

    public String getPatientAge() {
        return patientAge;
    }

    public void setPatientAge(String patientAge) {
        this.patientAge = patientAge;
    }

    public String getWardName() {
        return wardName;
    }

    public void setWardName(String wardName) {
        this.wardName = wardName;
    }

    public String getBedName() {
        return bedName;
    }

    public void setBedName(String bedName) {
        this.bedName = bedName;
    }

    public LocalDateTime getResultTime() {
        return resultTime;
    }

    public void setResultTime(LocalDateTime resultTime) {
        this.resultTime = resultTime;
    }

    public String getResultType() {
        return resultType;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    public String getDataCategory() {
        return dataCategory;
    }

    public void setDataCategory(String dataCategory) {
        this.dataCategory = dataCategory;
    }

    public String getHisTarget() {
        return hisTarget;
    }

    public void setHisTarget(String hisTarget) {
        this.hisTarget = hisTarget;
    }

    public String getHisRequestNo() {
        return hisRequestNo;
    }

    public void setHisRequestNo(String hisRequestNo) {
        this.hisRequestNo = hisRequestNo;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    public void setRawMessage(String rawMessage) {
        this.rawMessage = rawMessage;
    }

    public List<Hl7Observation> getObservations() {
        return observations;
    }

    public void setObservations(List<Hl7Observation> observations) {
        this.observations = observations;
    }
}
