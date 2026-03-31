package com.zhunyintech.inmehl7.client.export;

import com.zhunyintech.inmehl7.client.model.MedicalDataItem;
import com.zhunyintech.inmehl7.client.model.MedicalDataRecord;
import com.zhunyintech.inmehl7.client.ventilator.VentilatorRecordSupport;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DataExportService {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String KEY_DATE = "__date";
    private static final String KEY_TIME = "__time";
    private static final DataFormatter FORMATTER = new DataFormatter(Locale.CHINA);

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

    public void exportVentilatorTemplate(List<MedicalDataRecord> records,
                                         Path target,
                                         Path templatePath,
                                         String orgName) throws IOException {
        ensureParent(target);
        if (templatePath == null || !Files.exists(templatePath)) {
            throw new IOException("呼吸机导出模板不存在: " + templatePath);
        }
        List<MedicalDataRecord> sortedRecords = sortRecords(records);
        List<VentilatorRecordSupport.Snapshot> snapshots = new ArrayList<>();
        for (MedicalDataRecord record : sortedRecords) {
            snapshots.add(VentilatorRecordSupport.buildSnapshot(record));
        }
        try (InputStream in = Files.newInputStream(templatePath);
             Workbook workbook = WorkbookFactory.create(in)) {
            fillSheet1(resolveSheet(workbook, 0, "Sheet1"), sortedRecords, snapshots);
            fillSheet2(resolveSheet(workbook, 1, "Sheet2"), sortedRecords, snapshots);
            fillSheet3(resolveSheet(workbook, 2, "Sheet3"), sortedRecords, snapshots, orgName);
            try (OutputStream out = Files.newOutputStream(target)) {
                workbook.write(out);
            }
        } catch (Exception ex) {
            if (ex instanceof IOException) {
                throw (IOException) ex;
            }
            throw new IOException("生成呼吸机导出文件失败: " + ex.getMessage(), ex);
        }
    }

    private void fillSheet1(Sheet sheet,
                            List<MedicalDataRecord> records,
                            List<VentilatorRecordSupport.Snapshot> snapshots) {
        if (sheet == null) {
            return;
        }
        Integer dateRowIndex = null;
        Integer timeRowIndex = null;
        Map<String, Integer> metricRows = new LinkedHashMap<>();
        int lastDataCol = 1;
        for (int rowIndex = 0; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) {
                continue;
            }
            lastDataCol = Math.max(lastDataCol, Math.max(1, row.getLastCellNum() - 1));
            String label = cellText(row.getCell(0));
            String metricKey = VentilatorRecordSupport.resolveTemplateMetricKey(label);
            if (metricKey != null) {
                metricRows.putIfAbsent(metricKey, rowIndex);
            }
            String normalized = normalizeLabel(label);
            if ("日期".equals(normalized)) {
                dateRowIndex = rowIndex;
            } else if ("时间".equals(normalized)) {
                timeRowIndex = rowIndex;
            }
        }
        if (dateRowIndex == null || timeRowIndex == null) {
            return;
        }
        int clearTo = Math.max(lastDataCol, records.size());
        clearRowValues(sheet.getRow(dateRowIndex), 1, clearTo);
        clearRowValues(sheet.getRow(timeRowIndex), 1, clearTo);
        for (Integer rowIndex : metricRows.values()) {
            clearRowValues(sheet.getRow(rowIndex), 1, clearTo);
        }

        for (int i = 0; i < records.size(); i++) {
            int columnIndex = i + 1;
            LocalDateTime recordTime = recordTime(records.get(i));
            if (recordTime != null) {
                writeNumeric(sheet.getRow(dateRowIndex), columnIndex, toExcelDate(recordTime.toLocalDate()), 1);
                writeNumeric(sheet.getRow(timeRowIndex), columnIndex, toExcelTime(recordTime.toLocalTime()), 1);
            }
            VentilatorRecordSupport.Snapshot snapshot = snapshots.get(i);
            for (Map.Entry<String, Integer> entry : metricRows.entrySet()) {
                Row row = sheet.getRow(entry.getValue());
                writeValue(row, columnIndex, blankToNull(snapshot.get(entry.getKey())), 1);
            }
        }
    }

    private void fillSheet2(Sheet sheet,
                            List<MedicalDataRecord> records,
                            List<VentilatorRecordSupport.Snapshot> snapshots) {
        if (sheet == null) {
            return;
        }
        Row headerRow = findHeaderRow(sheet);
        if (headerRow == null) {
            return;
        }
        Row templateRow = firstDataTemplateRow(sheet, headerRow.getRowNum() + 1);
        Map<String, Integer> columns = findColumns(headerRow);
        int startRow = headerRow.getRowNum() + 1;
        clearDataRows(sheet, startRow, templateRow);
        for (int i = 0; i < records.size(); i++) {
            Row row = prepareRow(sheet, startRow + i, templateRow);
            MedicalDataRecord record = records.get(i);
            VentilatorRecordSupport.Snapshot snapshot = snapshots.get(i);
            LocalDateTime recordTime = recordTime(record);
            if (recordTime != null) {
                writeNumeric(row, columns.getOrDefault(KEY_DATE, 0), toExcelDate(recordTime.toLocalDate()), 0);
                writeNumeric(row, columns.getOrDefault(KEY_TIME, 1), toExcelTime(recordTime.toLocalTime()), 1);
            }
            writeValue(row, 2, String.valueOf(records.size() - i), 2);
            writeSnapshotColumns(row, columns, snapshot);
        }
    }

    private void fillSheet3(Sheet sheet,
                            List<MedicalDataRecord> records,
                            List<VentilatorRecordSupport.Snapshot> snapshots,
                            String orgName) {
        if (sheet == null) {
            return;
        }
        if (!isBlank(orgName)) {
            Row orgRow = getOrCreateRow(sheet, 0);
            writeValue(orgRow, 0, orgName.trim(), 0);
        }
        Row headerRow = findHeaderRow(sheet);
        if (headerRow == null) {
            return;
        }
        if (!records.isEmpty() && headerRow.getRowNum() > 0) {
            Row patientRow = getOrCreateRow(sheet, headerRow.getRowNum() - 1);
            writeValue(patientRow, 0, buildPatientSummary(records.get(0)), 0);
        }
        Row templateRow = firstDataTemplateRow(sheet, headerRow.getRowNum() + 1);
        Map<String, Integer> columns = findColumns(headerRow);
        int startRow = headerRow.getRowNum() + 1;
        clearDataRows(sheet, startRow, templateRow);
        for (int i = 0; i < records.size(); i++) {
            Row row = prepareRow(sheet, startRow + i, templateRow);
            MedicalDataRecord record = records.get(i);
            VentilatorRecordSupport.Snapshot snapshot = snapshots.get(i);
            LocalDateTime recordTime = recordTime(record);
            if (recordTime != null) {
                writeNumeric(row, columns.getOrDefault(KEY_DATE, 0), toExcelDate(recordTime.toLocalDate()), 0);
                writeNumeric(row, columns.getOrDefault(KEY_TIME, 1), toExcelTime(recordTime.toLocalTime()), 1);
            }
            writeSnapshotColumns(row, columns, snapshot);
        }
    }

    private void writeSnapshotColumns(Row row,
                                      Map<String, Integer> columns,
                                      VentilatorRecordSupport.Snapshot snapshot) {
        for (Map.Entry<String, Integer> entry : columns.entrySet()) {
            String key = entry.getKey();
            if (KEY_DATE.equals(key) || KEY_TIME.equals(key)) {
                continue;
            }
            writeValue(row, entry.getValue(), VentilatorRecordSupport.valueOrDash(snapshot, key), entry.getValue());
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

    private List<MedicalDataRecord> sortRecords(List<MedicalDataRecord> records) {
        List<MedicalDataRecord> sorted = new ArrayList<>();
        if (records != null) {
            sorted.addAll(records);
        }
        sorted.sort(Comparator.comparing(this::recordTime, Comparator.nullsLast(Comparator.reverseOrder())));
        return sorted;
    }

    private LocalDateTime recordTime(MedicalDataRecord record) {
        if (record == null) {
            return null;
        }
        if (record.getReportTime() != null) {
            return record.getReportTime();
        }
        if (record.getObservationTime() != null) {
            return record.getObservationTime();
        }
        return record.getReceiveTime();
    }

    private Sheet resolveSheet(Workbook workbook, int index, String preferredName) {
        if (workbook == null) {
            return null;
        }
        Sheet byName = workbook.getSheet(preferredName);
        if (byName != null) {
            return byName;
        }
        return workbook.getNumberOfSheets() > index ? workbook.getSheetAt(index) : null;
    }

    private Row findHeaderRow(Sheet sheet) {
        for (int rowIndex = 0; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) {
                continue;
            }
            String first = normalizeLabel(cellText(row.getCell(0)));
            String second = normalizeLabel(cellText(row.getCell(1)));
            if (first.contains("日期") && second.contains("时间")) {
                return row;
            }
        }
        return null;
    }

    private Map<String, Integer> findColumns(Row headerRow) {
        Map<String, Integer> columns = new LinkedHashMap<>();
        if (headerRow == null) {
            return columns;
        }
        for (Cell cell : headerRow) {
            String label = cellText(cell);
            String normalized = normalizeLabel(label);
            if ("日期".equals(normalized)) {
                columns.put(KEY_DATE, cell.getColumnIndex());
                continue;
            }
            if ("时间".equals(normalized)) {
                columns.put(KEY_TIME, cell.getColumnIndex());
                continue;
            }
            String key = VentilatorRecordSupport.resolveTemplateMetricKey(label);
            if (key != null) {
                columns.put(key, cell.getColumnIndex());
            }
        }
        return columns;
    }

    private Row firstDataTemplateRow(Sheet sheet, int startRow) {
        for (int rowIndex = startRow; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row != null) {
                return row;
            }
        }
        return sheet.getRow(startRow);
    }

    private void clearDataRows(Sheet sheet, int startRow, Row templateRow) {
        if (sheet == null) {
            return;
        }
        for (int rowIndex = startRow; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) {
                continue;
            }
            short lastCellNum = row.getLastCellNum();
            short templateLastCell = templateRow == null ? -1 : templateRow.getLastCellNum();
            int clearTo = Math.max(lastCellNum, templateLastCell);
            for (int col = 0; col < clearTo; col++) {
                Cell cell = row.getCell(col);
                if (cell != null) {
                    cell.setBlank();
                }
            }
        }
    }

    private Row prepareRow(Sheet sheet, int rowIndex, Row templateRow) {
        Row row = getOrCreateRow(sheet, rowIndex);
        if (templateRow != null) {
            row.setHeight(templateRow.getHeight());
            int maxCell = templateRow.getLastCellNum();
            for (int col = 0; col < maxCell; col++) {
                Cell templateCell = templateRow.getCell(col);
                Cell cell = row.getCell(col);
                if (cell == null) {
                    cell = row.createCell(col);
                }
                if (templateCell != null) {
                    cell.setCellStyle(templateCell.getCellStyle());
                }
                cell.setBlank();
            }
        }
        return row;
    }

    private Row getOrCreateRow(Sheet sheet, int rowIndex) {
        Row row = sheet.getRow(rowIndex);
        return row == null ? sheet.createRow(rowIndex) : row;
    }

    private void clearRowValues(Row row, int startCol, int endCol) {
        if (row == null) {
            return;
        }
        for (int col = startCol; col <= endCol; col++) {
            Cell cell = ensureCell(row, col, 1);
            cell.setBlank();
        }
    }

    private void writeNumeric(Row row, int columnIndex, double value, int templateColIndex) {
        Cell cell = ensureCell(row, columnIndex, templateColIndex);
        cell.setCellValue(value);
    }

    private void writeValue(Row row, int columnIndex, String value, int templateColIndex) {
        Cell cell = ensureCell(row, columnIndex, templateColIndex);
        if (isBlank(value)) {
            cell.setBlank();
            return;
        }
        Double numericValue = parseNumber(value);
        if (numericValue != null && !looksLikeRatio(value)) {
            cell.setCellValue(numericValue);
        } else {
            cell.setCellType(CellType.STRING);
            cell.setCellValue(value);
        }
    }

    private Cell ensureCell(Row row, int columnIndex, int templateColIndex) {
        Cell cell = row.getCell(columnIndex);
        if (cell != null) {
            return cell;
        }
        cell = row.createCell(columnIndex);
        int templateIndex = templateColIndex;
        if (templateIndex < 0) {
            templateIndex = Math.max(0, Math.min(columnIndex, row.getLastCellNum() - 1));
        }
        Cell templateCell = templateIndex >= 0 ? row.getCell(templateIndex) : null;
        if (templateCell == null && row.getLastCellNum() > 0) {
            templateCell = row.getCell(Math.max(0, row.getLastCellNum() - 1));
        }
        if (templateCell != null) {
            cell.setCellStyle(templateCell.getCellStyle());
        }
        return cell;
    }

    private String buildPatientSummary(MedicalDataRecord record) {
        if (record == null) {
            return "患者姓名：    性别    年龄    床号    住院号";
        }
        return "患者姓名：" + valueOrDash(record.getPatientName())
            + "    性别 " + valueOrDash(record.getPatientGender())
            + "    年龄 " + valueOrDash(record.getPatientAge())
            + "    床号 " + valueOrDash(joinText(record.getWardName(), record.getBedName(), " / "))
            + "    住院号 " + valueOrDash(firstNonBlank(record.getVisitId(), record.getPatientId(), record.getReportNo()));
    }

    private double toExcelDate(LocalDate date) {
        return date == null ? 0d : 25569d + date.toEpochDay();
    }

    private double toExcelTime(LocalTime time) {
        return time == null ? 0d : time.toSecondOfDay() / 86400d;
    }

    private String cellText(Cell cell) {
        return cell == null ? null : FORMATTER.formatCellValue(cell);
    }

    private String normalizeLabel(String label) {
        if (label == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(label.length());
        for (char ch : label.toCharArray()) {
            if (Character.isLetterOrDigit(ch) || Character.UnicodeScript.of(ch) == Character.UnicodeScript.HAN) {
                sb.append(Character.toUpperCase(ch));
            }
        }
        return sb.toString().toUpperCase(Locale.ROOT);
    }

    private Double parseNumber(String value) {
        if (isBlank(value)) {
            return null;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (Exception ignored) {
            return null;
        }
    }

    private boolean looksLikeRatio(String value) {
        return value != null && value.contains(":");
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

    private String blankToNull(String value) {
        return isBlank(value) ? null : value.trim();
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value.trim();
            }
        }
        return null;
    }

    private String joinText(String left, String right, String joiner) {
        if (isBlank(left) && isBlank(right)) {
            return null;
        }
        if (isBlank(left)) {
            return right.trim();
        }
        if (isBlank(right)) {
            return left.trim();
        }
        return left.trim() + joiner + right.trim();
    }

    private String valueOrDash(String value) {
        return isBlank(value) ? "---" : value.trim();
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
