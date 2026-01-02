package esi.bigdata.backend.service;

import esi.bigdata.backend.model.FraudAlert;
import esi.bigdata.backend.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerService {
    private final FraudAlertService fraudAlertService;
    private final WebSocketService webSocketService;
    private final TransactionService transactionService;
    private final ObjectMapper objectMapper;
    private final KafkaMonitoringService kafkaMonitoringService;

    private static final DateTimeFormatter[] FORMATTERS = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    private LocalDateTime parseTimestamp(String timestampStr) {
        String cleaned = timestampStr.trim();
        for (DateTimeFormatter formatter : FORMATTERS) {
            try {
                return LocalDateTime.parse(cleaned, formatter);
            } catch (DateTimeParseException e) {
            }
        }
        log.error("Failed to parse timestamp: {}", timestampStr);
        return LocalDateTime.now();
    }

    @KafkaListener(topics = "all_transactions", groupId = "fraud-detection-backend-all")
    public void consumeAllTransactions(String message) {
        long startTime = System.currentTimeMillis();
        try {
            log.debug("Received transaction: {}", message);
            JsonNode json = objectMapper.readTree(message);

            Transaction transaction = new Transaction();
            transaction.setTransactionId(json.get("Transaction_ID").asText());
            transaction.setUserId(json.get("User_ID").asText());
            transaction.setTransactionType(json.has("Transaction_Type") ? json.get("Transaction_Type").asText() : "UNKNOWN");
            transaction.setTransactionAmount(json.get("Transaction_Amount").asDouble());
            transaction.setEventTime(parseTimestamp(json.get("event_time").asText()));
            transaction.setFraudFlag(json.get("fraud_flag").asInt());
            transaction.setFraudScore(json.get("fraud_score").asDouble());
            transaction.setLocation(json.has("Location") ? json.get("Location").asText() : "UNKNOWN");

            if (json.has("Device_Type")) {
                transaction.setDeviceType(json.get("Device_Type").asText());
            }
            if (json.has("Merchant_Category")) {
                transaction.setMerchantCategory(json.get("Merchant_Category").asText());
            }
            if (json.has("Account_Balance")) {
                transaction.setAccountBalance(json.get("Account_Balance").asDouble());
            }
            if (json.has("Risk_Score")) {
                transaction.setRiskScore(json.get("Risk_Score").asDouble());
            }
            if (json.has("Avg_Transaction_Amount_10m")) {
                transaction.setAvgTransactionAmount10m(json.get("Avg_Transaction_Amount_10m").asDouble());
            }
            if (json.has("Failed_Transaction_Count_10m")) {
                transaction.setFailedTransactionCount10m(json.get("Failed_Transaction_Count_10m").asInt());
            }

            transactionService.saveTransaction(transaction);
            kafkaMonitoringService.recordProcessingTime(System.currentTimeMillis() - startTime);

            webSocketService.sendMetricsUpdate();
            webSocketService.sendSystemHealthUpdate();

            log.info("Transaction saved: {} (Fraud: {})", transaction.getTransactionId(), transaction.getFraudFlag());

        } catch (Exception e) {
            log.error("Error processing transaction: {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = "fraud_alerts", groupId = "fraud-detection-backend-fraud")
    public void consumeFraudAlert(String message) {
        long startTime = System.currentTimeMillis();
        try {
            log.info("Received FRAUD ALERT: {}", message);
            JsonNode json = objectMapper.readTree(message);

            FraudAlert alert = new FraudAlert();
            alert.setTransactionId(json.get("Transaction_ID").asText());
            alert.setUserId(json.get("User_ID").asText());
            alert.setTransactionType(json.has("Transaction_Type") ? json.get("Transaction_Type").asText() : "UNKNOWN");
            alert.setTransactionAmount(json.get("Transaction_Amount").asDouble());
            alert.setEventTime(parseTimestamp(json.get("event_time").asText()));
            alert.setFraudFlag(json.get("fraud_flag").asInt());
            alert.setFraudScore(json.get("fraud_score").asDouble());
            alert.setLocation(json.has("Location") ? json.get("Location").asText() : "UNKNOWN");

            FraudAlert savedAlert = fraudAlertService.saveFraudAlert(alert);

            kafkaMonitoringService.recordProcessingTime(System.currentTimeMillis() - startTime);

            webSocketService.sendFraudAlert(savedAlert);
            webSocketService.sendTimeSeriesUpdate();
            webSocketService.sendHighRiskUsersUpdate();

            log.info("Fraud alert processed: {}", savedAlert.getTransactionId());
        } catch (Exception e) {
            log.error("Error processing fraud alert: {}", e.getMessage(), e);
        }
    }
}