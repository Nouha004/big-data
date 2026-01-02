package esi.bigdata.backend.repository;

import esi.bigdata.backend.model.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface TransactionRepo extends JpaRepository<Transaction, Long> {
    List<Transaction> findTop100ByOrderByEventTimeDesc();

    Long countByEventTimeAfter(LocalDateTime startTime);

    @Query("SELECT COUNT(t) FROM Transaction t WHERE t.fraudFlag = 1 AND t.eventTime >= :startTime")
    Long countFraudTransactionsSince(@Param("startTime") LocalDateTime startTime);

    @Query("SELECT AVG(t.fraudScore) FROM Transaction t WHERE t.eventTime >= :startTime")
    Double averageFraudScoreSince(@Param("startTime") LocalDateTime startTime);

    @Query(value = "SELECT DATE_TRUNC('hour', event_time) as hour, " +
            "COUNT(*) as total, " +
            "SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) as fraud_count " +
            "FROM transactions WHERE event_time >= :startTime " +
            "GROUP BY hour ORDER BY hour",
            nativeQuery = true)
    List<Object[]> getHourlyStats(@Param("startTime") LocalDateTime startTime);
}
