package com.zhunyintech.inmehl7.client.hl7;

import com.zhunyintech.inmehl7.client.model.Hl7Observation;
import com.zhunyintech.inmehl7.client.model.Hl7ResultRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Hl7MessageParser {

    private static final DateTimeFormatter ACK_TIME = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public ParseResult parseOru(String message, String receiverApp, String receiverFacility) {
        if (message == null || message.trim().isEmpty()) {
            return ParseResult.fail("Empty HL7 message");
        }
        List<String> segments = splitSegments(message);
        String[] msh = findSegmentFields(segments, "MSH");
        if (msh == null) {
            return ParseResult.fail("MSH segment missing");
        }
        String messageType = field(msh, 8);
        String controlId = field(msh, 9);
        if (messageType == null || !messageType.startsWith("ORU^R01")) {
            String ack = buildAck(msh, controlId, "AR", "Unsupported messageType", receiverApp, receiverFacility);
            return ParseResult.reject(ack, "Unsupported message type: " + messageType);
        }

        String[] pid = findSegmentFields(segments, "PID");
        String[] pv1 = findSegmentFields(segments, "PV1");
        String[] obr = findSegmentFields(segments, "OBR");
        List<String[]> obxList = findSegmentFieldList(segments, "OBX");

        Hl7ResultRecord record = new Hl7ResultRecord();
        record.setRecordId(UUID.randomUUID().toString());
        record.setMessageType(componentPart(messageType, 0));
        record.setTriggerEvent(componentPart(messageType, 1));
        record.setHl7Version(field(msh, 11));
        record.setSourceApplication(field(msh, 2));
        record.setSourceFacility(field(msh, 3));
        record.setMessageControlId(controlId);
        record.setPatientId(cleanComposite(field(pid, 3)));
        record.setPatientName(cleanComposite(field(pid, 5)));
        record.setPatientAge(firstNonBlank(cleanComposite(field(pid, 7)), cleanComposite(field(pid, 6))));
        record.setPatientGender(firstNonBlank(field(pid, 8), field(pid, 7)));
        record.setSampleNo(cleanComposite(firstNonBlank(field(obr, 3), field(obr, 2))));
        record.setResultTime(parseHl7Time(field(obr, 7)));
        record.setWardName(parseWardName(pv1));
        record.setBedName(parseBedName(pv1));
        record.setRawMessage(message);

        int sortNo = 1;
        for (String[] obx : obxList) {
            Hl7Observation observation = new Hl7Observation();
            String identifier = field(obx, 3);
            observation.setItemCode(cleanComposite(identifier));
            observation.setItemName(firstNonBlank(compositePart(identifier, 1), identifier));
            observation.setValueType(field(obx, 2));
            observation.setValue(firstNonBlank(field(obx, 5), field(obx, 4)));
            observation.setNumericValue(parseNumericValue(observation.getValue()));
            observation.setUnit(field(obx, 6));
            observation.setReferenceRange(field(obx, 7));
            observation.setAbnormalFlag(field(obx, 8));
            observation.setObservedAt(parseHl7Time(firstNonBlank(field(obx, 14), field(obr, 7))));
            observation.setSortNo(sortNo++);
            record.getObservations().add(observation);
        }

        String ack = buildAck(msh, controlId, "AA", "OK", receiverApp, receiverFacility);
        return ParseResult.success(record, ack);
    }

    private List<String> splitSegments(String message) {
        String normalized = message.replace('\n', '\r');
        String[] arr = normalized.split("\r");
        List<String> result = new ArrayList<>();
        for (String line : arr) {
            if (line != null && !line.trim().isEmpty()) {
                result.add(line.trim());
            }
        }
        return result;
    }

    private String[] findSegmentFields(List<String> segments, String segmentName) {
        for (String segment : segments) {
            if (segment.startsWith(segmentName + "|")) {
                return segment.split("\\|", -1);
            }
        }
        return null;
    }

    private List<String[]> findSegmentFieldList(List<String> segments, String segmentName) {
        List<String[]> list = new ArrayList<>();
        for (String segment : segments) {
            if (segment.startsWith(segmentName + "|")) {
                list.add(segment.split("\\|", -1));
            }
        }
        return list;
    }

    private String buildAck(String[] msh,
                            String messageControlId,
                            String ackCode,
                            String text,
                            String receiverApp,
                            String receiverFacility) {
        String sendingApp = field(msh, 2);
        String sendingFacility = field(msh, 3);
        String version = firstNonBlank(field(msh, 11), "2.3");
        String controlId = firstNonBlank(messageControlId, UUID.randomUUID().toString().replace("-", ""));
        String ackMessageId = UUID.randomUUID().toString().replace("-", "");
        String now = ACK_TIME.format(LocalDateTime.now());

        StringBuilder sb = new StringBuilder();
        sb.append("MSH|^~\\&|")
            .append(firstNonBlank(receiverApp, "INMEHL7CLIENT")).append("|")
            .append(firstNonBlank(receiverFacility, "INMEHL7")).append("|")
            .append(firstNonBlank(sendingApp, "DEVICE")).append("|")
            .append(firstNonBlank(sendingFacility, "DEVICE")).append("|")
            .append(now).append("||ACK^R01|")
            .append(ackMessageId).append("|P|")
            .append(version).append("\r");
        sb.append("MSA|").append(ackCode).append("|").append(controlId);
        if (text != null && !text.isEmpty()) {
            sb.append("|").append(text);
        }
        sb.append("\r");
        return sb.toString();
    }

    private String field(String[] fields, int index) {
        if (fields == null || index < 0 || index >= fields.length) {
            return null;
        }
        String value = fields[index];
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private String cleanComposite(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        String[] parts = value.split("\\^");
        return parts.length == 0 ? value : parts[0];
    }

    private String compositePart(String value, int index) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        String[] parts = value.split("\\^");
        if (index < 0 || index >= parts.length) {
            return null;
        }
        String part = parts[index];
        return part == null || part.trim().isEmpty() ? null : part.trim();
    }

    private String componentPart(String value, int index) {
        return compositePart(value, index);
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

    private LocalDateTime parseHl7Time(String value) {
        if (value == null || value.trim().isEmpty()) {
            return LocalDateTime.now();
        }
        String t = value.trim();
        try {
            if (t.length() >= 14) {
                return LocalDateTime.parse(t.substring(0, 14), DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            }
            if (t.length() >= 12) {
                return LocalDateTime.parse(t.substring(0, 12), DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
            }
            if (t.length() >= 8) {
                return LocalDateTime.parse(t.substring(0, 8) + "000000", DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            }
        } catch (Exception ignored) {
        }
        return LocalDateTime.now();
    }

    private Double parseNumericValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (Exception ignored) {
            return null;
        }
    }

    private String parseWardName(String[] pv1) {
        String location = field(pv1, 3);
        if (location == null) {
            return null;
        }
        String[] parts = location.split("\\^", -1);
        if (parts.length < 3) {
            return null;
        }
        String[] sub = parts[2].split("&", -1);
        if (sub.length < 2) {
            return null;
        }
        return firstNonBlank(sub[1]);
    }

    private String parseBedName(String[] pv1) {
        String location = field(pv1, 3);
        if (location == null) {
            return null;
        }
        String[] parts = location.split("\\^", -1);
        if (parts.length < 3) {
            return null;
        }
        String[] sub = parts[2].split("&", -1);
        if (sub.length == 0) {
            return null;
        }
        return firstNonBlank(sub[0]);
    }

    public static class ParseResult {
        private final boolean success;
        private final boolean accepted;
        private final Hl7ResultRecord record;
        private final String ackMessage;
        private final String errorMessage;

        private ParseResult(boolean success, boolean accepted, Hl7ResultRecord record, String ackMessage, String errorMessage) {
            this.success = success;
            this.accepted = accepted;
            this.record = record;
            this.ackMessage = ackMessage;
            this.errorMessage = errorMessage;
        }

        public static ParseResult success(Hl7ResultRecord record, String ack) {
            return new ParseResult(true, true, record, ack, null);
        }

        public static ParseResult reject(String ack, String message) {
            return new ParseResult(false, false, null, ack, message);
        }

        public static ParseResult fail(String message) {
            return new ParseResult(false, false, null, null, message);
        }

        public boolean isSuccess() {
            return success;
        }

        public boolean isAccepted() {
            return accepted;
        }

        public Hl7ResultRecord getRecord() {
            return record;
        }

        public String getAckMessage() {
            return ackMessage;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }
}
