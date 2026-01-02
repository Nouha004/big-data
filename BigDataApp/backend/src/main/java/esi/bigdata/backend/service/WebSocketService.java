package esi.bigdata.backend.service;

import esi.bigdata.backend.dto.DashboardKPI;
import esi.bigdata.backend.dto.TimeSeries;
import esi.bigdata.backend.model.FraudAlert;
import esi.bigdata.backend.dto.SystemMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class WebSocketService {
    private final SimpMessagingTemplate messagingTemplate;
    private final FraudAlertService fraudAlertService;

    public void sendFraudAlert(FraudAlert alert) {
        try {
            messagingTemplate.convertAndSend("/topic/fraud-alerts", alert);
            log.info("Sent fraud alert via WebSocket: {}", alert.getTransactionId());
        } catch (Exception e) {
            log.error("Error sending WebSocket message: {}", e.getMessage());
        }
    }

    public void sendMetricsUpdate() {
        try {
            DashboardKPI kpis = fraudAlertService.getKPIs(24);
            messagingTemplate.convertAndSend("/topic/metrics", kpis);
            log.debug("Sent KPIs update via WebSocket");
        } catch (Exception e) {
            log.error("Error sending metrics update: {}", e.getMessage());
        }
    }
    public void sendTimeSeriesUpdate() {
        try {
            List<TimeSeries> timeSeries = fraudAlertService.getTimeSeriesData(12);
            messagingTemplate.convertAndSend("/topic/timeseries", timeSeries);
            log.debug("Sent time series update via WebSocket");
        } catch (Exception e) {
            log.error("Error sending time series update: {}", e.getMessage());
        }
    }

    public void sendHighRiskUsersUpdate() {
        try {
            List<Map<String, Object>> highRiskUsers = fraudAlertService.getHighRiskUsers(24);
            messagingTemplate.convertAndSend("/topic/high-risk-users", highRiskUsers);
            log.debug("Sent high risk users update via WebSocket");
        } catch (Exception e) {
            log.error("Error sending high risk users update: {}", e.getMessage());
        }
    }
    public void sendSystemHealthUpdate() {
        try {
            SystemMetrics metrics = fraudAlertService.getSystemMetrics();
            messagingTemplate.convertAndSend("/topic/system-health", metrics);
            log.debug("Sent system health update");
        } catch (Exception e) {
            log.error("Error sending system health update", e);
        }
    }
}
