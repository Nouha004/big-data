package esi.bigdata.backend.repository;

import esi.bigdata.backend.model.FraudAlert;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface FraudAlertsRepo extends JpaRepository<FraudAlert, Long> {
    List<FraudAlert> findTop100ByOrderByEventTimeDesc();
    List<FraudAlert> findTop50ByFraudFlagOrderByEventTimeDesc(Integer fraudFlag);

    @Query("SELECT COUNT(f) FROM FraudAlert f WHERE f.fraudFlag = 1 AND f.eventTime >= :startTime")
    Long countFraudAlertsSince(@Param("startTime") LocalDateTime startTime);

    Long countByEventTimeAfter(LocalDateTime startTime);

    @Query("SELECT AVG(f.fraudScore) FROM FraudAlert f WHERE f.fraudFlag = 1 AND f.eventTime >= :startTime")
    Double averageFraudScoreSince(@Param("startTime") LocalDateTime startTime);

    @Query("SELECT f.userId, COUNT(f) as fraudCount FROM FraudAlert f " +
            "WHERE f.fraudFlag = 1 AND f.eventTime >= :startTime " +
            "GROUP BY f.userId ORDER BY fraudCount DESC")
    List<Object[]> findTopFraudUsers(@Param("startTime") LocalDateTime startTime);

    @Query(value = "SELECT DATE_TRUNC('hour', event_time) as hour, " +
            "COUNT(*) as total, " +
            "SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) as fraud_count " +
            "FROM fraud_alerts WHERE event_time >= :startTime " +
            "GROUP BY hour ORDER BY hour",
            nativeQuery = true)
    List<Object[]> getHourlyStats(@Param("startTime") LocalDateTime startTime);
}
