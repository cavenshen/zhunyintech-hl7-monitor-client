package com.zhunyintech.inmehl7.client.export;

import com.zhunyintech.inmehl7.client.model.MedicalDataItem;
import com.zhunyintech.inmehl7.client.model.MedicalDataRecord;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class DataExportService {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void exportCsv(List<MedicalDataRecord> records, Path target) throws IOException {
        ensureParent(target);
        StringBuilder sb = new StringBuilder();
        sb.append('\uFEFF');
        appendHeader(sb);
        appendRows(records, sb);
        Files.writeString(target, sb.toString(), StandardCharsets.UTF_8);
    }

    public void exportRawTxt(MedicalDataRecord record, Path target) throws IOException {
        ensureParent(target);
        String raw = record == null || record.getRawPayload() == null ? "" : record.getRawPayload();
        Files.writeString(target, raw, StandardCharsets.UTF_8);
    }

    public void exportXlsx(List<MedicalDataRecord> records, Path target) throws IOException {
        ensureParent(target);
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("records");
            int rowIdx = 0;
            Row header = sheet.createRow(rowIdx++);
            String[] titles = titles();
            for (int i = 0; i < titles.length; i++) {
                header.createCell(i).setCellValue(titles[i]);
            }
            for (MedicalDataRecord record : records) {
                List<MedicalDataItem> items = record.getItems();
                if (items == null || items.isEmpty()) {
                    Row row = sheet.createRow(rowIdx++);
                    writeRow(record, null, row);
                    continue;
                }
                for (MedicalDataItem item : items) {
                    Row row = sheet.createRow(rowIdx++);
                    writeRow(record, item, row);
                }
            }
            for (int i = 0; i < titles.length; i++) {
                sheet.autoSizeColumn(i);
            }
            try (OutputStream outputStream = Files.newOutputStream(target)) {
                workbook.write(outputStream);
            }
        }
    }

    private void ensureParent(Path target) throws IOException {
        if (target == null) {
            return;
        }
        Path parent = target.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
    }

    private void appendHeader(StringBuilder sb) {
        String[] titles = titles();
        for (int i = 0; i < titles.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(csv(titles[i]));
        }
        sb.append(System.lineSeparator());
    }

    private void appendRows(List<MedicalDataRecord> records, StringBuilder sb) {
        if (records == null) {
            return;
        }
        for (MedicalDataRecord record : records) {
            List<MedicalDataItem> items = record.getItems();
            if (items == null || items.isEmpty()) {
                appendRow(sb, record, null);
                continue;
            }
            for (MedicalDataItem item : items) {
                appendRow(sb, record, item);
            }
        }
    }

    private void appendRow(StringBuilder sb, MedicalDataRecord record, MedicalDataItem item) {
        String[] values = values(record, item);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(csv(values[i]));
        }
        sb.append(System.lineSeparator());
    }

    private void writeRow(MedicalDataRecord record, MedicalDataItem item, Row row) {
        String[] values = values(record, item);
        for (int i = 0; i < values.length; i++) {
            row.createCell(i).setCellValue(values[i] == null ? "" : values[i]);
        }
    }

    private String[] values(MedicalDataRecord record, MedicalDataItem item) {
        return new String[] {
            text(record == null ? null : record.getReportTime()),
            safe(record == null ? null : record.getDeviceName()),
            safe(record == null ? null : record.getDeviceId()),
            safe(record == null ? null : record.getDeviceType()),
            safe(record == null ? null : record.getPatientName()),
            safe(record == null ? null : record.getPatientId()),
            safe(record == null ? null : record.getWardName()),
            safe(record == null ? null : record.getBedName()),
            safe(record == null ? null : record.getMessageType()),
            safe(record == null ? null : record.getTriggerEvent()),
            safe(record == null ? null : record.getMessageControlId()),
            safe(record == null ? null : record.getRecordStatus()),
            item == null ? "" : safe(item.getItemCode()),
            item == null ? "" : safe(item.getItemName()),
            item == null ? "" : safe(item.getValueText()),
            item == null ? "" : safe(item.getUnit()),
            item == null ? "" : safe(item.getRefRange()),
            item == null ? "" : safe(item.getAbnormalFlag())
        };
    }

    private String[] titles() {
        return new String[] {
            "报告时间",
            "设备名称",
            "设备ID",
            "设备类型",
            "患者姓名",
            "患者ID",
            "病房",
            "病床",
            "消息类型",
            "触发事件",
            "控制号",
            "记录状态",
            "项目编码",
            "项目名称",
            "结果值",
            "单位",
            "参考范围",
            "异常标记"
        };
    }

    private String csv(String value) {
        String safe = value == null ? "" : value;
        if (safe.contains(",") || safe.contains("\"") || safe.contains("\n") || safe.contains("\r")) {
            return "\"" + safe.replace("\"", "\"\"") + "\"";
        }
        return safe;
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    private String text(LocalDateTime value) {
        return value == null ? "" : TS.format(value);
    }
}
