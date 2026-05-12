package com.ptt.client.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ptt.client.model.SensorDataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import jakarta.annotation.PreDestroy;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Subscribes to gateway STOMP topics ({@code /topic/tags}, {@code /topic/calcs}) and forwards
 * to the same WebSocket + H2 path as Kafka—implements the gateway → client leg without Kafka.
 */
@Service
public class GatewayStompBridgeService {

    private static final Logger logger = LoggerFactory.getLogger(GatewayStompBridgeService.class);

    private final WebSocketService webSocketService;
    private final DatabaseService databaseService;
    private final ObjectMapper objectMapper;

    @Value("${client.live.source:kafka}")
    private String liveSource;

    @Value("${gateway.internal.stomp.base-url:}")
    private String gatewayBaseUrl;

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private ExecutorService executor;
    private WebSocketStompClient stompClient;
    private volatile StompSession session;

    public GatewayStompBridgeService(WebSocketService webSocketService, DatabaseService databaseService,
            ObjectMapper objectMapper) {
        this.webSocketService = webSocketService;
        this.databaseService = databaseService;
        this.objectMapper = objectMapper;
    }

    @jakarta.annotation.PostConstruct
    public void start() {
        if (!isGatewayLiveSource()) {
            logger.info("Gateway STOMP bridge disabled (client.live.source={})", liveSource.trim());
            return;
        }
        String base = gatewayBaseUrl == null ? "" : gatewayBaseUrl.trim();
        if (base.isEmpty()) {
            logger.warn(
                    "client.live.source includes gateway but gateway.internal.stomp.base-url is empty; bridge not started");
            return;
        }

        List<Transport> transports = List.of(new WebSocketTransport(new StandardWebSocketClient()));
        SockJsClient sockJsClient = new SockJsClient(transports);
        stompClient = new WebSocketStompClient(sockJsClient);
        stompClient.setMessageConverter(new MappingJackson2MessageConverter());

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "gateway-stomp-bridge");
            t.setDaemon(true);
            return t;
        });
        executor.submit(this::runReconnectLoop);
        logger.info("Gateway STOMP bridge scheduled (base URL: {})", base);
    }

    private boolean isGatewayLiveSource() {
        String s = liveSource.trim().toLowerCase(Locale.ROOT);
        return "gateway".equals(s) || "both".equals(s);
    }

    private void runReconnectLoop() {
        while (!stopped.get()) {
            try {
                connectAndListen();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!stopped.get()) {
                    logger.warn("Gateway STOMP error: {} — retry in 5s", e.getMessage());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        logger.info("Gateway STOMP bridge loop ended");
    }

    private void connectAndListen() throws Exception {
        String base = gatewayBaseUrl.trim().replaceAll("/+$", "");
        String sockJsUrl = base + "/ws-endpoint";

        StompSessionHandlerAdapter handler = new StompSessionHandlerAdapter() {
            @Override
            public void afterConnected(@NonNull StompSession s, @NonNull StompHeaders connectedHeaders) {
                session = s;
                logger.info("Gateway STOMP session connected");
                s.subscribe("/topic/tags", frameHandler("tag"));
                s.subscribe("/topic/calcs", frameHandler("calc"));
            }

            @Override
            public void handleTransportError(@NonNull StompSession s, @NonNull Throwable exception) {
                logger.error("Gateway STOMP transport error", exception);
            }
        };

        Future<StompSession> future = stompClient.connectAsync(sockJsUrl, handler);
        StompSession s = future.get(60, TimeUnit.SECONDS);
        session = s;

        while (!stopped.get() && s.isConnected()) {
            Thread.sleep(500);
        }
        if (s.isConnected()) {
            try {
                s.disconnect();
            } catch (Exception ignored) {
            }
        }
    }

    private StompFrameHandler frameHandler(String kind) {
        return new StompFrameHandler() {
            @Override
            @NonNull
            public Type getPayloadType(@NonNull StompHeaders headers) {
                return Map.class;
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleFrame(@NonNull StompHeaders headers, Object payload) {
                if (!(payload instanceof Map)) {
                    return;
                }
                forwardGatewayPayload(kind, (Map<String, Object>) payload);
            }
        };
    }

    private void forwardGatewayPayload(String kind, Map<String, Object> m) {
        try {
            Object tagId = m.get("tagID");
            Object value = m.get("value");
            Object ts = m.get("timestamp");
            if (tagId == null || value == null) {
                return;
            }
            double cur = ((Number) value).doubleValue();
            String timeStr = ts != null ? ts.toString() : "";
            String tagname = String.valueOf(tagId);

            SensorDataRecord record = new SensorDataRecord();
            record.setTagname(tagname);
            record.setCurValue(cur);
            record.setTime(timeStr);
            record.setGw(0);

            String json = objectMapper.writeValueAsString(record);
            webSocketService.broadcast(json);
            databaseService.insertRecord(record);
        } catch (Exception e) {
            logger.debug("Skip gateway {} frame: {}", kind, e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        stopped.set(true);
        if (session != null && session.isConnected()) {
            try {
                session.disconnect();
            } catch (Exception ignored) {
            }
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
