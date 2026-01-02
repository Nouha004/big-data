package esi.bigdata.backend.service;

import esi.bigdata.backend.dto.DashboardKPI;
import esi.bigdata.backend.dto.TimeSeries;
import esi.bigdata.backend.model.FraudAlert;
import esi.bigdata.backend.dto.SystemMetrics;
import esi.bigdata.backend.repository.FraudAlertsRepo;
import esi.bigdata.backend.repository.TransactionRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudAlertService {
    private final FraudAlertsRepo fraudRepo;
    private final TransactionRepo transactionRepo;
    private final KafkaMonitoringService kafkaMonitoringService;

    @Transactional
    public FraudAlert saveFraudAlert(FraudAlert alert){
        try {
            return fraudRepo.save(alert);
        } catch (Exception e) {
            log.error("Error saving fraud alert: {}", e.getMessage(), e);
            throw e;
        }
    }

    public List<FraudAlert> getRecentAlerts(int limit) {
        return fraudRepo.findTop100ByOrderByEventTimeDesc();
    }

    public List<FraudAlert> getRecentFraudAlerts() {
        return fraudRepo.findTop50ByFraudFlagOrderByEventTimeDesc(1);
    }

    public DashboardKPI getKPIs(int hours) {
        LocalDateTime since = LocalDateTime.now().minusHours(hours);

        Long totalTransactions = transactionRepo.countByEventTimeAfter(since);

        Long fraudCount = transactionRepo.countFraudTransactionsSince(since);

        Double avgFraudScore = transactionRepo.averageFraudScoreSince(since);

        double fraudRate = totalTransactions > 0
                ? (fraudCount * 100.0 / totalTransactions)
                : 0.0;

        DashboardKPI kpis = new DashboardKPI();
        kpis.setTotalTransactions(totalTransactions);
        kpis.setFraudDetected(fraudCount);
        kpis.setFraudRate(Math.round(fraudRate * 100.0) / 100.0);
        kpis.setAverageFraudScore(avgFraudScore != null ? Math.round(avgFraudScore * 1000.0) / 1000.0 : 0.0);
        kpis.setTimeRangeHours(hours);

        return kpis;
    }

    public List<TimeSeries> getTimeSeriesData(int hours) {
        LocalDateTime since = LocalDateTime.now().minusHours(hours);
        List<Object[]> rawData = transactionRepo.getHourlyStats(since);

        List<TimeSeries> result = new ArrayList<>();
        for (Object[] row : rawData) {
            TimeSeries data = new TimeSeries();
            data.setTimestamp(((LocalDateTime) row[0]));
            data.setTotalTransactions(((Number) row[1]).longValue());
            data.setFraudCount(((Number) row[2]).longValue());
            result.add(data);
        }
        return result;
    }

    public List<Map<String, Object>> getHighRiskUsers(int hours) {
        LocalDateTime since = LocalDateTime.now().minusHours(hours);
        List<Object[]> rawData = fraudRepo.findTopFraudUsers(since);

        List<Map<String, Object>> result = new ArrayList<>();
        int count = 0;
        for (Object[] row : rawData) {
            if (count++ >= 10) break;

            Map<String, Object> user = new HashMap<>();
            user.put("userId", row[0]);
            user.put("fraudCount", row[1]);
            result.add(user);
        }

        return result;
    }

    public SystemMetrics getSystemMetrics() {
        SystemMetrics metrics = new SystemMetrics();
        metrics.setProcessingTimeMs(kafkaMonitoringService.getAverageProcessingTime());
        metrics.setMessagesProcessed(kafkaMonitoringService.processedCount.get());
        metrics.setStatus("HEALTHY");
        metrics.setLastUpdated(LocalDateTime.now());
        return metrics;
    }
}