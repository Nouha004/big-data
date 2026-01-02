package esi.bigdata.backend.controller;

import esi.bigdata.backend.dto.DashboardKPI;
import esi.bigdata.backend.dto.TimeSeries;
import esi.bigdata.backend.model.FraudAlert;
import esi.bigdata.backend.dto.SystemMetrics;
import esi.bigdata.backend.model.Transaction;
import esi.bigdata.backend.service.FraudAlertService;
import esi.bigdata.backend.service.TransactionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/fraud")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class FraudAlertController {
    private final FraudAlertService fraudAlertService;
    private final TransactionService transactionService;


    @GetMapping("/alerts/recent")
    public ResponseEntity<List<FraudAlert>> getRecentAlerts() {
        return ResponseEntity.ok(fraudAlertService.getRecentAlerts(100));
    }

    @GetMapping("/alerts/fraud")
    public ResponseEntity<List<FraudAlert>> getFraudAlerts() {
        return ResponseEntity.ok(fraudAlertService.getRecentFraudAlerts());
    }
    @GetMapping("/transactions/recent")
    public ResponseEntity<List<Transaction>> getRecentTransactions() {
        return ResponseEntity.ok(transactionService.getRecentTransactions(100));
    }


    @GetMapping("/kpis")
    public ResponseEntity<DashboardKPI> getKPIs(
            @RequestParam(defaultValue = "24") int hours) {
        return ResponseEntity.ok(fraudAlertService.getKPIs(hours));
    }

    @GetMapping("/timeseries")
    public ResponseEntity<List<TimeSeries>> getTimeSeries(
            @RequestParam(defaultValue = "24") int hours) {
        return ResponseEntity.ok(fraudAlertService.getTimeSeriesData(hours));
    }

    @GetMapping("/users/high-risk")
    public ResponseEntity<List<Map<String, Object>>> getHighRiskUsers(
            @RequestParam(defaultValue = "24") int hours) {
        return ResponseEntity.ok(fraudAlertService.getHighRiskUsers(hours));
    }

    @GetMapping("/metrics")
    public ResponseEntity<SystemMetrics> getSystemMetrics() {
        return ResponseEntity.ok(fraudAlertService.getSystemMetrics());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        long totalTransactions = transactionService.getTotalTransactionCount(24);
        long fraudCount = fraudAlertService.getRecentFraudAlerts().size();

        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "Fraud Detection API",
                "totalTransactions24h", totalTransactions,
                "fraudAlerts24h", fraudCount
        ));
    }
}
