package com.zhunyintech.inmehl7.client.serial;

import com.fazecast.jSerialComm.SerialPort;
import com.zhunyintech.inmehl7.client.log.RuntimeLogger;
import com.zhunyintech.inmehl7.client.model.MonitorListenerProfile;
import com.zhunyintech.inmehl7.client.model.MonitorProtocolType;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class SerialProtocolListener {

    public interface MessageHandler {
        void onMessage(MonitorProtocolType protocolType, String portName, String payload);
    }

    private final MonitorListenerProfile profile;
    private final RuntimeLogger logger;
    private final MessageHandler handler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private SerialPort serialPort;

    public SerialProtocolListener(MonitorListenerProfile profile, RuntimeLogger logger, MessageHandler handler) {
        this.profile = profile;
        this.logger = logger;
        this.handler = handler;
    }

    public void start() {
        if (profile == null || profile.getProtocolType() == MonitorProtocolType.HL7) {
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        executor.submit(this::runLoop);
    }

    public void stop() {
        running.set(false);
        try {
            if (serialPort != null) {
                serialPort.closePort();
            }
        } catch (Exception ignored) {
        }
        executor.shutdownNow();
    }

    public boolean isRunning() {
        return running.get();
    }

    private void runLoop() {
        String portName = profile.getSerialPortName();
        if (portName == null || portName.trim().isEmpty()) {
            running.set(false);
            logger.warn(profile.getProtocolType().getDisplayName() + "监听未启动：串口号为空");
            return;
        }
        try {
            serialPort = SerialPort.getCommPort(portName.trim());
            serialPort.setComPortParameters(
                positive(profile.getBaudRate(), 9600),
                positive(profile.getDataBits(), 8),
                resolveStopBits(profile.getStopBits()),
                resolveParity(profile.getParity())
            );
            serialPort.setComPortTimeouts(SerialPort.TIMEOUT_READ_BLOCKING, positive(profile.getReadTimeoutMs(), 1000), 0);
            if (!serialPort.openPort()) {
                running.set(false);
                logger.warn(profile.getProtocolType().getDisplayName() + "监听启动失败：无法打开串口 " + portName);
                return;
            }
            logger.info(profile.getProtocolType().getDisplayName() + "监听已启动: " + portName);
            byte[] delimiter = resolveDelimiter(profile.getFrameDelimiter());
            Charset charset = resolveCharset(profile.getCharsetName());
            try (InputStream input = serialPort.getInputStream()) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                int value;
                while (running.get() && (value = input.read()) != -1) {
                    buffer.write(value);
                    if (endsWith(buffer.toByteArray(), delimiter)) {
                        emit(buffer, charset, delimiter.length);
                    }
                }
                emit(buffer, charset, 0);
            }
        } catch (Exception ex) {
            if (running.get()) {
                logger.error(profile.getProtocolType().getDisplayName() + "串口监听异常", ex);
            }
        } finally {
            running.set(false);
            try {
                if (serialPort != null) {
                    serialPort.closePort();
                }
            } catch (Exception ignored) {
            }
            logger.info(profile.getProtocolType().getDisplayName() + "监听已停止");
        }
    }

    private void emit(ByteArrayOutputStream buffer, Charset charset, int trimBytes) {
        if (buffer == null || buffer.size() == 0) {
            return;
        }
        byte[] bytes = buffer.toByteArray();
        int length = Math.max(0, bytes.length - trimBytes);
        String payload = new String(bytes, 0, length, charset).trim();
        buffer.reset();
        if (payload.isEmpty() || handler == null) {
            return;
        }
        handler.onMessage(profile.getProtocolType(), profile.getSerialPortName(), payload);
    }

    private boolean endsWith(byte[] source, byte[] suffix) {
        if (source == null || suffix == null || source.length < suffix.length) {
            return false;
        }
        for (int i = 0; i < suffix.length; i++) {
            if (source[source.length - suffix.length + i] != suffix[i]) {
                return false;
            }
        }
        return true;
    }

    private byte[] resolveDelimiter(String value) {
        String text = value == null ? "" : value.trim().toUpperCase();
        if ("CR".equals(text)) {
            return new byte[] {0x0D};
        }
        if ("LF".equals(text)) {
            return new byte[] {0x0A};
        }
        if ("NONE".equals(text)) {
            return new byte[] {0x0D, 0x0A};
        }
        return new byte[] {0x0D, 0x0A};
    }

    private Charset resolveCharset(String value) {
        try {
            return Charset.forName(value == null || value.trim().isEmpty() ? "UTF-8" : value.trim());
        } catch (Exception ignored) {
            return StandardCharsets.UTF_8;
        }
    }

    private int resolveStopBits(Integer value) {
        int stopBits = positive(value, 1);
        if (stopBits == 2) {
            return SerialPort.TWO_STOP_BITS;
        }
        return SerialPort.ONE_STOP_BIT;
    }

    private int resolveParity(String value) {
        String parity = value == null ? "" : value.trim().toUpperCase();
        if ("ODD".equals(parity)) {
            return SerialPort.ODD_PARITY;
        }
        if ("EVEN".equals(parity)) {
            return SerialPort.EVEN_PARITY;
        }
        if ("MARK".equals(parity)) {
            return SerialPort.MARK_PARITY;
        }
        if ("SPACE".equals(parity)) {
            return SerialPort.SPACE_PARITY;
        }
        return SerialPort.NO_PARITY;
    }

    private int positive(Integer value, int defaultValue) {
        return value == null || value <= 0 ? defaultValue : value;
    }
}
