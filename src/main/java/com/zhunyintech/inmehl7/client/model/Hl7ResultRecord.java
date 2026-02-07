package com.zhunyintech.inmehl7.client.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Hl7ResultRecord {
    private String recordId;
    private String messageControlId;
    private String sampleNo;
    private String patientId;
    private String patientName;
    private LocalDateTime resultTime;
    private String resultType = "ORU_R01";
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

