package esi.bigdata.backend.service;

import esi.bigdata.backend.model.Transaction;
import esi.bigdata.backend.repository.TransactionRepo;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {
    private final TransactionRepo transactionRepo;

    @Transactional
    public Transaction saveTransaction(Transaction transaction) {
        try {
            return transactionRepo.save(transaction);
        } catch (Exception e) {
            log.error("Error saving transaction: {}", e.getMessage(), e);
            throw e;
        }
    }

    public List<Transaction> getRecentTransactions(int limit) {
        return transactionRepo.findTop100ByOrderByEventTimeDesc();
    }

    public Long getTotalTransactionCount(int hours) {
        LocalDateTime since = LocalDateTime.now().minusHours(hours);
        Long count = transactionRepo.countByEventTimeAfter(since);
        return count != null ? count : 0L;
    }

    public Long getFraudTransactionCount(int hours) {
        LocalDateTime since = LocalDateTime.now().minusHours(hours);
        Long count = transactionRepo.countFraudTransactionsSince(since);
        return count != null ? count : 0L;
    }
}
