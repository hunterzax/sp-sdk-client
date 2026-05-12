package com.ptt.client.service;

import com.ptt.client.util.LogSanitizer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.InetSocketAddress;

/**
 * Service to manage the WebSocket server.
 */
@Service
public class WebSocketService {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketService.class);
    private RealtimeWebSocketServer server;

    @Value("${client.websocket.enabled:true}")
    private boolean websocketEnabled;

    @Value("${websocket.port:8887}")
    private int port;

    @PostConstruct
    public void startServer() {
        if (!websocketEnabled) {
            logger.info("WebSocket server disabled (client.websocket.enabled=false)");
            return;
        }
        server = new RealtimeWebSocketServer(new InetSocketAddress(port));
        server.start();
        logger.info("WebSocket Server started on port: {}", port);
    }

    @PreDestroy
    public void stopServer() {
        if (server != null) {
            try {
                server.stop();
                logger.info("WebSocket Server stopped.");
            } catch (Exception e) {
                logger.error("Error stopping WebSocket server", e);
            }
        }
    }

    public void broadcast(String message) {
        if (!websocketEnabled || server == null) {
            return;
        }
        server.broadcast(message);
    }

    public boolean isWebsocketEnabled() {
        return websocketEnabled;
    }

    // Inner class for WebSocket Server
    private static class RealtimeWebSocketServer extends WebSocketServer {

        public RealtimeWebSocketServer(InetSocketAddress address) {
            super(address);
        }

        @Override
        public void onOpen(WebSocket conn, ClientHandshake handshake) {
            String remote = conn.getRemoteSocketAddress() != null
                    ? LogSanitizer.sanitize(conn.getRemoteSocketAddress().toString())
                    : "unknown";
            logger.info("New WebSocket connection: {}", remote);
        }

        @Override
        public void onClose(WebSocket conn, int code, String reason, boolean remote) {
            String addr = conn.getRemoteSocketAddress() != null
                    ? LogSanitizer.sanitize(conn.getRemoteSocketAddress().toString())
                    : "unknown";
            logger.info("WebSocket closed: {}", addr);
        }

        @Override
        public void onMessage(WebSocket conn, String message) {
            // CWE-117: sanitize untrusted client input before logging to prevent log injection.
            // Assign to a local variable so static-analysis can verify the taint is broken.
            final String safeMessage = LogSanitizer.sanitize(message);
            logger.info("Message from client: {}", safeMessage);
        }

        @Override
        public void onError(WebSocket conn, Exception ex) {
            // CWE-117 & CWE-476: conn and ex.getMessage() may be null;
            // sanitize all untrusted values before logging.
            // Assign to local variables so static-analysis can verify the taint is broken.
            final String remote;
            if (conn != null && conn.getRemoteSocketAddress() != null) {
                remote = LogSanitizer.sanitize(conn.getRemoteSocketAddress().toString());
            } else {
                remote = "server";
            }
            final String errorMsg;
            if (ex != null && ex.getMessage() != null) {
                errorMsg = LogSanitizer.sanitize(ex.getMessage());
            } else {
                errorMsg = "unknown";
            }
            logger.error("WebSocket error from [{}]: {}", remote, errorMsg);
        }

        @Override
        public void onStart() {
            logger.info("WebSocket Server initialized.");
        }
    }
}
