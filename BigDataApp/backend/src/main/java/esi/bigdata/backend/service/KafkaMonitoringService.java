package esi.bigdata.backend.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;

import org.springframework.stereotype.Service;


import java.util.concurrent.atomic.AtomicLong;


@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaMonitoringService {
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    final AtomicLong processedCount = new AtomicLong(0);

    public void recordProcessingTime(long processingTimeMs) {
        totalProcessingTime.addAndGet(processingTimeMs);
        processedCount.incrementAndGet();
    }


    public long getAverageProcessingTime() {
        long count = processedCount.get();
        if (count == 0) return 0;
        return totalProcessingTime.get() / count;
    }

    public void resetStats() {
        totalProcessingTime.set(0);
        processedCount.set(0);
    }
}
