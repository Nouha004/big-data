package esi.bigdata.backend.dto;

import lombok.Data;

@Data
public class DashboardKPI {
    private Long totalTransactions;
    private Long fraudDetected;
    private Double fraudRate;
    private Double averageFraudScore;
    private Integer timeRangeHours;
}
