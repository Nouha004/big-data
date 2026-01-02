package esi.bigdata.backend.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name="transactions", indexes = {
        @Index(name = "idx_txn_user_id", columnList = "userId"),
        @Index(name = "idx_txn_event_time", columnList = "eventTime"),
        @Index(name = "idx_txn_fraud_flag", columnList = "fraudFlag") })
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Transaction {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String transactionId;

    @Column(nullable = false)
    private String userId;

    @Column(nullable = false)
    private Double transactionAmount;

    @Column
    private String transactionType;

    @Column(nullable = false)
    private LocalDateTime eventTime;

    @Column(nullable = false)
    private Integer fraudFlag;

    @Column(nullable = false)
    private Double fraudScore;

    private String location;

    private String deviceType;

    private String merchantCategory;

    private Double accountBalance;

    private Double riskScore;

    private Double avgTransactionAmount10m;

    private Integer failedTransactionCount10m;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}
