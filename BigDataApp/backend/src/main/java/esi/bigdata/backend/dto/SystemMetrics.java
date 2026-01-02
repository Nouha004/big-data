package esi.bigdata.backend.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SystemMetrics {
    private Long processingTimeMs;
    private Long messagesProcessed;
    private String status = "HEALTHY";
    private LocalDateTime lastUpdated = LocalDateTime.now();
}
