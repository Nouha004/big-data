package esi.bigdata.backend.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TimeSeries {
    private LocalDateTime timestamp;
    private Long totalTransactions;
    private Long fraudCount;
}
