package com.ptt.client.controller;

import com.ptt.client.service.DatabaseService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api")
public class SensorDataController {

    // CWE-87531: allowlist pattern for tag names — reject anything that could be
    // used for XSS or injection before it reaches the database or response body
    private static final Pattern SAFE_TAG = Pattern.compile("^[\\w.\\-]{1,255}$");

    private final DatabaseService databaseService;
    private final int websocketPort;
    private final String liveSource;
    private final String gatewayStompPublicUrl;
    private final boolean websocketEnabled;
    private final boolean dbWriterEnabled;
    private final boolean kafkaForwardEnabled;

    @Autowired
    public SensorDataController(DatabaseService databaseService,
            @Value("${websocket.port:8887}") int websocketPort,
            @Value("${client.live.source:kafka}") String liveSource,
            @Value("${gateway.stomp.public-url:}") String gatewayStompPublicUrl,
            @Value("${client.websocket.enabled:true}") boolean websocketEnabled,
            @Value("${dbwriter.enabled:false}") boolean dbWriterEnabled,
            @Value("${kafka.forward.enabled:false}") boolean kafkaForwardEnabled) {
        this.databaseService = databaseService;
        this.websocketPort = websocketPort;
        this.liveSource = liveSource;
        this.gatewayStompPublicUrl = gatewayStompPublicUrl;
        this.websocketEnabled = websocketEnabled;
        this.dbWriterEnabled = dbWriterEnabled;
        this.kafkaForwardEnabled = kafkaForwardEnabled;
    }

    /**
     * Browser-visible WebSocket URL (same host as the page, port from server config).
     */
    @GetMapping("/config")
    public Map<String, String> clientConfig(HttpServletRequest request) {
        String host = resolveBrowserHost(request);
        String wsScheme = request.isSecure() ? "wss" : "ws";
        Map<String, String> out = new HashMap<>();
        out.put("websocketEnabled", Boolean.toString(websocketEnabled));
        out.put("dbWriterEnabled", Boolean.toString(dbWriterEnabled));
        out.put("kafkaForwardEnabled", Boolean.toString(kafkaForwardEnabled));
        if (websocketEnabled) {
            out.put("websocketUrl", wsScheme + "://" + host + ":" + websocketPort);
        } else {
            out.put("websocketUrl", "");
        }
        out.put("liveSource", liveSource.trim());
        String pub = gatewayStompPublicUrl == null ? "" : gatewayStompPublicUrl.trim();
        if (!pub.isEmpty()) {
            String base = pub.replaceAll("/+$", "");
            out.put("gatewaySockJsUrl", base + "/ws-endpoint");
        }
        return out;
    }

    private static String resolveBrowserHost(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-Host");
        if (forwarded != null && !forwarded.isBlank()) {
            String first = forwarded.split(",")[0].trim();
            int colon = first.indexOf(':');
            return colon > 0 ? first.substring(0, colon) : first;
        }
        return request.getServerName();
    }

    @GetMapping("/tags")
    public List<String> getTags() {
        return databaseService.getAllTags();
    }

    @GetMapping("/history")
    public List<Map<String, Object>> getHistory(
            @RequestParam("tag") String tag,
            @RequestParam(value = "days", defaultValue = "7") int days) {
        // CWE-87531: validate tag parameter to prevent XSS/injection via reflected user input
        validateTag(tag);
        return databaseService.getSensorHistory(tag, days);
    }

    @GetMapping("/heatmap")
    public List<Map<String, Object>> getHeatmap(@RequestParam("tag") String tag) {
        // CWE-87531: validate tag parameter to prevent XSS/injection via reflected user input
        validateTag(tag);
        return databaseService.getSensorHeatmap(tag);
    }

    /**
     * Validates that the tag parameter matches the safe allowlist pattern.
     * Rejects blank values or any string containing characters outside
     * word characters, dots, and hyphens (prevents XSS and injection).
     *
     * @param tag the raw request parameter value
     * @throws ResponseStatusException HTTP 400 if the tag is invalid
     */
    private static void validateTag(String tag) {
        if (tag == null || tag.isBlank() || !SAFE_TAG.matcher(tag).matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid tag parameter");
        }
    }
}
