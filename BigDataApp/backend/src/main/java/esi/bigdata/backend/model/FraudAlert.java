package esi.bigdata.backend.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name="fraud_alerts", indexes = {
        @Index(name = "idx_user_id", columnList = "userId"),
        @Index(name = "idx_event_time", columnList = "eventTime"),
        @Index(name = "idx_fraud_flag", columnList = "fraudFlag")
})
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FraudAlert {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

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

    @Column(nullable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }

}
