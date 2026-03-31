package com.zhunyintech.inmehl7.client.hl7;

import com.zhunyintech.inmehl7.client.log.RuntimeLogger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MllpServer {

    private static final byte VT = 0x0b;
    private static final byte FS = 0x1c;
    private static final byte CR = 0x0d;

    private final int port;
    private final RuntimeLogger logger;
    private final MessageHandler messageHandler;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ServerSocket serverSocket;

    public MllpServer(int port, RuntimeLogger logger, MessageHandler messageHandler) {
        this.port = port;
        this.logger = logger;
        this.messageHandler = messageHandler;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        executorService.submit(() -> {
            try (ServerSocket ss = new ServerSocket(port)) {
                serverSocket = ss;
                logger.info("MLLP listener started at port " + port);
                while (running.get()) {
                    Socket socket = ss.accept();
                    executorService.submit(() -> handleClient(socket));
                }
            } catch (IOException ex) {
                boolean wasRunning = running.getAndSet(false);
                if (wasRunning) {
                    logger.error("MLLP server crashed", ex);
                } else {
                    logger.warn("MLLP listener stopped: " + ex.getMessage());
                }
            }
        });
    }

    public void stop() {
        running.set(false);
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
        executorService.shutdownNow();
        logger.info("MLLP listener stopped");
    }

    public boolean isRunning() {
        return running.get();
    }

    private void handleClient(Socket socket) {
        try (Socket client = socket;
             InputStream input = client.getInputStream();
             OutputStream output = client.getOutputStream()) {
            logger.info("MLLP client connected: " + client.getRemoteSocketAddress());
            ByteArrayOutputStream frameBuffer = new ByteArrayOutputStream();
            boolean inFrame = false;
            int b;
            while (running.get() && (b = input.read()) != -1) {
                byte ch = (byte) b;
                if (!inFrame) {
                    if (ch == VT) {
                        inFrame = true;
                        frameBuffer.reset();
                    }
                    continue;
                }
                if (ch == FS) {
                    int next = input.read();
                    if (next == CR) {
                        String message = frameBuffer.toString(StandardCharsets.UTF_8);
                        String ack = messageHandler.onMessage(buildContext(client, message));
                        if (ack != null && !ack.trim().isEmpty()) {
                            output.write(VT);
                            output.write(ack.getBytes(StandardCharsets.UTF_8));
                            output.write(FS);
                            output.write(CR);
                            output.flush();
                        }
                        inFrame = false;
                        frameBuffer.reset();
                        continue;
                    }
                }
                frameBuffer.write(ch);
            }
        } catch (Exception ex) {
            logger.warn("MLLP client disconnected: " + ex.getMessage());
        }
    }

    public interface MessageHandler {
        String onMessage(MessageContext context);
    }

    public static class MessageContext {
        private final int listenPort;
        private final String remoteAddress;
        private final String remoteIp;
        private final int remotePort;
        private final String hl7Message;

        public MessageContext(int listenPort, String remoteAddress, String remoteIp, int remotePort, String hl7Message) {
            this.listenPort = listenPort;
            this.remoteAddress = remoteAddress;
            this.remoteIp = remoteIp;
            this.remotePort = remotePort;
            this.hl7Message = hl7Message;
        }

        public int getListenPort() {
            return listenPort;
        }

        public String getRemoteAddress() {
            return remoteAddress;
        }

        public String getRemoteIp() {
            return remoteIp;
        }

        public int getRemotePort() {
            return remotePort;
        }

        public String getHl7Message() {
            return hl7Message;
        }
    }

    private MessageContext buildContext(Socket client, String message) {
        String remoteAddress = client.getRemoteSocketAddress() == null
            ? null
            : client.getRemoteSocketAddress().toString();
        String remoteIp = null;
        int remotePort = -1;
        if (client.getRemoteSocketAddress() instanceof InetSocketAddress address) {
            if (address.getAddress() != null) {
                remoteIp = address.getAddress().getHostAddress();
            }
            remotePort = address.getPort();
        }
        return new MessageContext(port, remoteAddress, remoteIp, remotePort, message);
    }
}
