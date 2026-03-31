package com.zhunyintech.inmehl7.client.ventilator;

import com.zhunyintech.inmehl7.client.model.MedicalDataItem;
import com.zhunyintech.inmehl7.client.model.MedicalDataRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class VentilatorRecordSupport {

    public static final String KEY_PEAK_PRESSURE = "peakPressure";
    public static final String KEY_MINUTE_VENTILATION = "minuteVentilation";
    public static final String KEY_TIDAL_VOLUME_EXP = "tidalVolumeExp";
    public static final String KEY_TOTAL_RATE = "totalRate";
    public static final String KEY_TIDAL_VOLUME_INSP = "tidalVolumeInsp";
    public static final String KEY_SPONTANEOUS_RATE = "spontaneousRate";
    public static final String KEY_RESISTANCE_INSP = "resistanceInsp";
    public static final String KEY_RESISTANCE_EXP = "resistanceExp";
    public static final String KEY_COMPLIANCE_DYN = "complianceDyn";
    public static final String KEY_RSBI = "rsbi";
    public static final String KEY_WORK_OF_BREATHING = "workOfBreathing";
    public static final String KEY_TIME_CONSTANT = "timeConstant";
    public static final String KEY_PLATEAU_PRESSURE = "plateauPressure";
    public static final String KEY_MEAN_PRESSURE = "meanPressure";
    public static final String KEY_PEEP = "peep";
    public static final String KEY_TVE_SPN = "tveSpn";
    public static final String KEY_TVE_PER_IBW = "tvePerIbw";
    public static final String KEY_MV_SPN = "mvSpn";
    public static final String KEY_MV_LEAK = "mvLeak";
    public static final String KEY_LEAK_PERCENT = "leakPercent";
    public static final String KEY_MANDATORY_RATE = "mandatoryRate";
    public static final String KEY_COMPLIANCE_STATIC = "complianceStatic";
    public static final String KEY_FIO2 = "fio2";
    public static final String KEY_IE_RATIO = "ieRatio";
    public static final String KEY_INSP_TIME = "inspTime";
    public static final String KEY_SPO2 = "spo2";
    public static final String KEY_PULSE_RATE = "pulseRate";

    private static final LinkedHashMap<String, List<String>> METRIC_RULES = new LinkedHashMap<>();
    private static final LinkedHashMap<String, String> DISPLAY_LABELS = new LinkedHashMap<>();
    private static final LinkedHashMap<String, String> DISPLAY_UNITS = new LinkedHashMap<>();
    private static final LinkedHashMap<String, String> TEMPLATE_LABEL_KEYS = new LinkedHashMap<>();
    private static final List<String> SUMMARY_KEYS = List.of(
        KEY_PEAK_PRESSURE,
        KEY_MINUTE_VENTILATION,
        KEY_TIDAL_VOLUME_EXP,
        KEY_TOTAL_RATE,
        KEY_PEEP,
        KEY_FIO2
    );
    private static final List<String> DETAIL_KEYS = List.of(
        KEY_PEAK_PRESSURE,
        KEY_MINUTE_VENTILATION,
        KEY_TIDAL_VOLUME_EXP,
        KEY_TOTAL_RATE,
        KEY_TIDAL_VOLUME_INSP,
        KEY_SPONTANEOUS_RATE,
        KEY_PLATEAU_PRESSURE,
        KEY_MEAN_PRESSURE,
        KEY_PEEP,
        KEY_MANDATORY_RATE,
        KEY_FIO2,
        KEY_IE_RATIO,
        KEY_INSP_TIME,
        KEY_RESISTANCE_INSP,
        KEY_RESISTANCE_EXP,
        KEY_COMPLIANCE_DYN,
        KEY_COMPLIANCE_STATIC,
        KEY_RSBI,
        KEY_WORK_OF_BREATHING,
        KEY_TIME_CONSTANT,
        KEY_TVE_SPN,
        KEY_TVE_PER_IBW,
        KEY_MV_SPN,
        KEY_MV_LEAK,
        KEY_LEAK_PERCENT,
        KEY_SPO2,
        KEY_PULSE_RATE
    );

    static {
        registerMetric(KEY_PEAK_PRESSURE, "峰值压", "cmH2O", "151972", "MDC_VENT_PRESS_AWAY", "PPEAK");
        registerMetric(KEY_MINUTE_VENTILATION, "分钟通气量", "L/min", "152000", "MDC_VENT_VOL_MINUTE_EXP", "MINUTEVENTILATION", "MV");
        registerMetric(KEY_TIDAL_VOLUME_EXP, "呼出潮气量", "mL", "152664", "MDC_VOL_AWAY_TIDAL_EXP", "TVE");
        registerMetric(KEY_TOTAL_RATE, "总频率", "bpm", "152490", "MDC_VENT_RESP_BTSD_PSAZC_RATE", "TOTRR");
        registerMetric(KEY_TIDAL_VOLUME_INSP, "吸入潮气量", "mL", "152660", "MDC_VOL_AWAY_TIDAL_INSP", "TVI");
        registerMetric(KEY_SPONTANEOUS_RATE, "自主频率", "bpm", "152538", "MDC_VENT_RESP_BTSD_PS_RATE", "SPONTRR");
        registerMetric(KEY_RESISTANCE_INSP, "Ri", "cmH2O/L/s", "151848", "MDC_RES_AWAY_INSP", "RI");
        registerMetric(KEY_RESISTANCE_EXP, "Re", "cmH2O/L/s", "151844", "MDC_RES_AWAY_EXP", "RE");
        registerMetric(KEY_COMPLIANCE_DYN, "Cdyn", "mL/cmH2O", "151692", "MDC_COMPL_LUNG_DYN", "CDYN");
        registerMetric(KEY_RSBI, "RSBI", "1/(min·L)", "152860", "MDC_RESP_RAPID_SHALLOW_BREATHING_INDEX", "RSBI");
        registerMetric(KEY_WORK_OF_BREATHING, "呼吸功", "J/min", "510", "MNDRY_WORK_OF_BREATHING_TOTAL", "WORKOFBREATHING");
        registerMetric(KEY_TIME_CONSTANT, "时间常数", "s", "189", "MNDRY_TIME_CONSTANT_EXP", "TIMECONSTANT");
        registerMetric(KEY_PLATEAU_PRESSURE, "平台压", "cmH2O", "151784", "MDC_PRESS_RESP_PLAT", "PLATEAUPRESSURE");
        registerMetric(KEY_MEAN_PRESSURE, "平均压", "cmH2O", "151819", "MDC_PRESS_AWAY_INSP_MEAN", "MEANPRESSURE");
        registerMetric(KEY_PEEP, "呼末正压", "cmH2O", "151976", "MDC_VENT_PRESS_AWAY_END_EXP_POS", "PEEP");
        registerMetric(KEY_TVE_SPN, "TVe spn", "mL", "152676", "MDC_VOL_AWAY_TIDAL_EXP_BTSD_PS", "TVESPN");
        registerMetric(KEY_TVE_PER_IBW, "TVe/IBW", "mL/kg", "153208", "MDC_VOL_AWAY_TIDAL_PER_IBW", "TVEIBW");
        registerMetric(KEY_MV_SPN, "MVspn", "L/min", "151880", "MDC_VOL_MINUTE_AWAY", "MVSPN");
        registerMetric(KEY_MV_LEAK, "MVleak", "L/min", "152432", "MDC_VENT_VOL_LEAK", "MVLEAK");
        registerMetric(KEY_LEAK_PERCENT, "Leak%", "%", "565", "MNDRY_VENT_LEAK_PERCENT", "LEAKPERCENT");
        registerMetric(KEY_MANDATORY_RATE, "机控频率", "bpm", "179", "MNDRY_BREATH_RATE_MAND", "MANDRR");
        registerMetric(KEY_COMPLIANCE_STATIC, "Cstat", "mL/cmH2O", "151696", "MDC_COMPL_LUNG_STATIC", "CSTAT");
        registerMetric(KEY_FIO2, "FiO2", "%", "152196", "MDC_CONC_AWAY_O2_INSP", "FIO2");
        registerMetric(KEY_IE_RATIO, "吸呼比", null, "151832", "MDC_RATIO_IE", "IERATIO");
        registerMetric(KEY_INSP_TIME, "吸气时间", "s", "152416", "MDC_VENT_TIME_PD_INSP", "INSPTIME");
        registerMetric(KEY_SPO2, "SpO2", "%", "MDC_PULS_OXIM_SAT_O2", "SPO2");
        registerMetric(KEY_PULSE_RATE, "PR", "1/min", "MDC_PULS_OXIM_PULS_RATE", "PULSERATE", "PR");

        registerTemplateLabel("峰值压", KEY_PEAK_PRESSURE);
        registerTemplateLabel("分钟通气量", KEY_MINUTE_VENTILATION);
        registerTemplateLabel("呼出潮气量", KEY_TIDAL_VOLUME_EXP);
        registerTemplateLabel("总频率", KEY_TOTAL_RATE);
        registerTemplateLabel("吸入潮气量", KEY_TIDAL_VOLUME_INSP);
        registerTemplateLabel("自主频率", KEY_SPONTANEOUS_RATE);
        registerTemplateLabel("RI", KEY_RESISTANCE_INSP);
        registerTemplateLabel("RE", KEY_RESISTANCE_EXP);
        registerTemplateLabel("CDYN", KEY_COMPLIANCE_DYN);
        registerTemplateLabel("CSTAT", KEY_COMPLIANCE_STATIC);
        registerTemplateLabel("RSBI", KEY_RSBI);
        registerTemplateLabel("呼吸功", KEY_WORK_OF_BREATHING);
        registerTemplateLabel("时间常数", KEY_TIME_CONSTANT);
        registerTemplateLabel("平台压", KEY_PLATEAU_PRESSURE);
        registerTemplateLabel("平均压", KEY_MEAN_PRESSURE);
        registerTemplateLabel("呼末正压", KEY_PEEP);
        registerTemplateLabel("机控频率", KEY_MANDATORY_RATE);
        registerTemplateLabel("吸呼比", KEY_IE_RATIO);
        registerTemplateLabel("吸气时间", KEY_INSP_TIME);
        registerTemplateLabel("TVESPN", KEY_TVE_SPN);
        registerTemplateLabel("TVEIBW", KEY_TVE_PER_IBW);
        registerTemplateLabel("MVSPN", KEY_MV_SPN);
        registerTemplateLabel("MVLEAK", KEY_MV_LEAK);
        registerTemplateLabel("LEAK", KEY_LEAK_PERCENT);
        registerTemplateLabel("FIO2", KEY_FIO2);
        registerTemplateLabel("SPO2", KEY_SPO2);
        registerTemplateLabel("PR", KEY_PULSE_RATE);
    }

    private VentilatorRecordSupport() {
    }

    public static boolean isVentilatorRecord(MedicalDataRecord record) {
        return record != null && "VENTILATOR".equalsIgnoreCase(blankToNull(record.getDeviceType()));
    }

    public static List<String> metricKeys() {
        return Collections.unmodifiableList(new ArrayList<>(METRIC_RULES.keySet()));
    }

    public static String resolveTemplateMetricKey(String label) {
        String normalized = normalizeToken(label);
        if (normalized.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> entry : TEMPLATE_LABEL_KEYS.entrySet()) {
            if (normalized.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static Snapshot buildSnapshot(MedicalDataRecord record) {
        return record == null ? new Snapshot() : buildSnapshot(record.getItems());
    }

    public static Snapshot buildSnapshot(Collection<MedicalDataItem> items) {
        Snapshot snapshot = new Snapshot();
        if (items == null || items.isEmpty()) {
            return snapshot;
        }
        Map<String, MedicalDataItem> matched = new LinkedHashMap<>();
        for (MedicalDataItem item : items) {
            if (item == null) {
                continue;
            }
            for (Map.Entry<String, List<String>> entry : METRIC_RULES.entrySet()) {
                String key = entry.getKey();
                if (matched.containsKey(key) || !matches(item, entry.getValue())) {
                    continue;
                }
                matched.put(key, item);
            }
        }

        for (String key : METRIC_RULES.keySet()) {
            MedicalDataItem item = matched.get(key);
            if (item == null) {
                continue;
            }
            String value = extractMetricValue(key, item);
            if (!isBlank(value)) {
                snapshot.put(key, value);
            }
        }

        if (isBlank(snapshot.get(KEY_TOTAL_RATE))) {
            Double mandatory = parseDouble(snapshot.get(KEY_MANDATORY_RATE));
            Double spontaneous = parseDouble(snapshot.get(KEY_SPONTANEOUS_RATE));
            if (mandatory != null && spontaneous != null) {
                snapshot.put(KEY_TOTAL_RATE, formatDecimal(mandatory + spontaneous));
            }
        }
        return snapshot;
    }

    public static String buildSummary(MedicalDataRecord record) {
        return buildSummary(buildSnapshot(record));
    }

    public static String buildSummary(Collection<MedicalDataItem> items) {
        return buildSummary(buildSnapshot(items));
    }

    public static String buildSummary(Snapshot snapshot) {
        if (snapshot == null || snapshot.isEmpty()) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        for (String key : SUMMARY_KEYS) {
            String value = blankToNull(snapshot.get(key));
            if (value == null) {
                continue;
            }
            String unit = DISPLAY_UNITS.get(key);
            String label = DISPLAY_LABELS.get(key);
            parts.add(label + " " + value + (isBlank(unit) ? "" : " " + unit));
        }
        return parts.isEmpty() ? null : String.join(" | ", parts);
    }

    public static String buildDetailBlock(Snapshot snapshot) {
        if (snapshot == null || snapshot.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("\u547c\u5438\u673a\u5173\u952e\u6307\u6807").append(System.lineSeparator());
        for (String key : DETAIL_KEYS) {
            String value = blankToNull(snapshot.get(key));
            if (value == null) {
                continue;
            }
            sb.append("- ").append(DISPLAY_LABELS.get(key)).append(": ").append(value);
            String unit = DISPLAY_UNITS.get(key);
            if (!isBlank(unit)) {
                sb.append(" ").append(unit);
            }
            sb.append(System.lineSeparator());
        }
        return sb.toString().trim();
    }

    public static String valueOrDash(Snapshot snapshot, String key) {
        String value = snapshot == null ? null : blankToNull(snapshot.get(key));
        return value == null ? "---" : value;
    }

    public static String displayLabel(String key) {
        return DISPLAY_LABELS.get(key);
    }

    private static void registerMetric(String key, String label, String unit, String... aliases) {
        DISPLAY_LABELS.put(key, label);
        DISPLAY_UNITS.put(key, unit);
        List<String> values = new ArrayList<>();
        if (aliases != null) {
            Collections.addAll(values, aliases);
        }
        METRIC_RULES.put(key, values);
    }

    private static void registerTemplateLabel(String label, String key) {
        TEMPLATE_LABEL_KEYS.put(normalizeToken(label), key);
    }

    private static boolean matches(MedicalDataItem item, List<String> aliases) {
        if (item == null || aliases == null || aliases.isEmpty()) {
            return false;
        }
        String code = normalizeToken(item.getItemCode());
        String name = normalizeToken(item.getItemName());
        for (String alias : aliases) {
            String token = normalizeToken(alias);
            if (token.isEmpty()) {
                continue;
            }
            if (token.equals(code) || token.equals(name)) {
                return true;
            }
            if (containsChinese(alias) && (code.contains(token) || name.contains(token))) {
                return true;
            }
        }
        return false;
    }

    private static String extractMetricValue(String key, MedicalDataItem item) {
        if (item == null) {
            return null;
        }
        if (KEY_RSBI.equals(key) && "INV".equalsIgnoreCase(blankToNull(item.getAbnormalFlag()))) {
            return null;
        }
        if (KEY_IE_RATIO.equals(key)) {
            return normalizeIeRatio(item.getValueText());
        }
        if (item.getNumericValue() != null) {
            return formatDecimal(item.getNumericValue());
        }
        String raw = blankToNull(item.getValueText());
        if (raw == null || "---".equals(raw)) {
            return null;
        }
        String normalizedRaw = normalizeToken(raw);
        String code = normalizeToken(item.getItemCode());
        if (KEY_RSBI.equals(key) && normalizedRaw.equals(code)) {
            return null;
        }
        return raw;
    }

    private static String normalizeIeRatio(String value) {
        String raw = blankToNull(value);
        if (raw == null) {
            return null;
        }
        raw = raw.replace("^", " ").replace("/", ":").replace("：", ":");
        raw = raw.replaceAll("\\s+", " ").trim();
        if (raw.contains(":")) {
            String[] parts = raw.split(":");
            List<String> cleaned = new ArrayList<>();
            for (String part : parts) {
                String token = blankToNull(part);
                if (token != null) {
                    cleaned.add(token);
                }
            }
            return cleaned.isEmpty() ? null : String.join(":", cleaned);
        }
        return raw;
    }

    private static String normalizeToken(String text) {
        if (text == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(text.length());
        for (char ch : text.toCharArray()) {
            if (Character.isLetterOrDigit(ch) || isChinese(ch)) {
                sb.append(Character.toUpperCase(ch));
            }
        }
        return sb.toString().toUpperCase(Locale.ROOT);
    }

    private static boolean isChinese(char ch) {
        Character.UnicodeScript script = Character.UnicodeScript.of(ch);
        return script == Character.UnicodeScript.HAN;
    }

    private static boolean containsChinese(String value) {
        if (value == null) {
            return false;
        }
        for (char ch : value.toCharArray()) {
            if (isChinese(ch)) {
                return true;
            }
        }
        return false;
    }

    private static Double parseDouble(String value) {
        if (isBlank(value)) {
            return null;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String formatDecimal(double value) {
        long longValue = (long) value;
        if (Math.abs(value - longValue) < 0.000001d) {
            return String.valueOf(longValue);
        }
        String text = String.format(Locale.ROOT, "%.4f", value);
        while (text.contains(".") && (text.endsWith("0") || text.endsWith("."))) {
            text = text.substring(0, text.length() - 1);
        }
        return text;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String blankToNull(String value) {
        return isBlank(value) ? null : value.trim();
    }

    public static final class Snapshot {
        private final Map<String, String> values = new LinkedHashMap<>();

        public void put(String key, String value) {
            if (isBlank(key) || isBlank(value)) {
                return;
            }
            values.put(key, value.trim());
        }

        public String get(String key) {
            return values.get(key);
        }

        public boolean isEmpty() {
            return values.isEmpty();
        }

        public Map<String, String> asMap() {
            return Collections.unmodifiableMap(values);
        }
    }
}
