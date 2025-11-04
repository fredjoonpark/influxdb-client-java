package com.influxdb.client.internal.flowable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.internal.AbstractWriteClient;
import com.influxdb.client.write.WriteParameters;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;
import io.reactivex.rxjava3.internal.subscriptions.BooleanSubscription;
import io.reactivex.rxjava3.subscribers.DefaultSubscriber;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Simplified tests for backpressure data capture functionality.
 * Focuses on core behavior: capture disabled/enabled and drop oldest vs latest strategies.
 */
class BackpressureDataCaptureTest {

    @Test
    public void testBackwardCompatibility() {
        AtomicInteger backpressureCount = new AtomicInteger(0);
        TestSubscriber<AbstractWriteClient.BatchWriteItem> testSubscriber = createTestSubscriber();

        // Test old constructor without capture parameter (should default to false)
        Flowable.fromPublisher(
                        createSequentialBatches(5)
                                .lift(new BackpressureBatchesBufferStrategy(
                                        3,
                                        bufferedPoints -> backpressureCount.incrementAndGet(),
                                        BackpressureOverflowStrategy.DROP_OLDEST)))
                .subscribe(testSubscriber);

        testSubscriber.request(10);
        testSubscriber.awaitDone(5, TimeUnit.SECONDS).assertNoErrors();

        Assertions.assertThat(backpressureCount.get()).isGreaterThan(0);
    }

    @Test
    public void testCaptureDisabled_NoDataCaptured() {
        int bufferSize = 3;
        List<String> capturedData = new ArrayList<>();

        TestSubscriber<AbstractWriteClient.BatchWriteItem> testSubscriber = createTestSubscriber();

        Flowable.fromPublisher(
                        createSequentialBatches(8)
                                .lift(new BackpressureBatchesBufferStrategy(
                                        bufferSize,
                                        capturedData::addAll,
                                        BackpressureOverflowStrategy.DROP_OLDEST,
                                        false))) // capture disabled
                .subscribe(testSubscriber);

        testSubscriber.request(1);
        testSubscriber.awaitDone(5, TimeUnit.SECONDS).assertNoErrors();

        // Should have backpressure but no captured data
        Assertions.assertThat(capturedData).isEmpty();
    }

    @Test
    public void testDropOldestVsDropLatest_CompareStrategies() {
        int bufferSize = 2;  // Small buffer: can hold batches 1,2 initially
        int batchCount = 6;  // Send batches 1,2,3,4,5,6
        
        // Test DROP_OLDEST
        // Expected: When batch 3 arrives, batch 1 gets dropped (oldest)
        // When batch 4 arrives, batch 2 gets dropped, etc.
        List<String> oldestDropped = new ArrayList<>();
        TestSubscriber<AbstractWriteClient.BatchWriteItem> oldestSubscriber = createTestSubscriber();
        
        Flowable.fromPublisher(
                        createSequentialBatches(batchCount)
                                .lift(new BackpressureBatchesBufferStrategy(
                                        bufferSize,
                                        oldestDropped::addAll,
                                        BackpressureOverflowStrategy.DROP_OLDEST,
                                        true)))
                .subscribe(oldestSubscriber);
        
        oldestSubscriber.request(1); // Only process 1 batch, causing backpressure
        oldestSubscriber.awaitDone(5, TimeUnit.SECONDS).assertNoErrors();
        
        // Test DROP_LATEST  
        // Expected: When batch 3 arrives, batch 3 gets dropped (latest)
        // When batch 4 arrives, batch 4 gets dropped, etc.
        List<String> latestDropped = new ArrayList<>();
        TestSubscriber<AbstractWriteClient.BatchWriteItem> latestSubscriber = createTestSubscriber();
        
        Flowable.fromPublisher(
                        createSequentialBatches(batchCount)
                                .lift(new BackpressureBatchesBufferStrategy(
                                        bufferSize,
                                        latestDropped::addAll,
                                        BackpressureOverflowStrategy.DROP_LATEST,
                                        true)))
                .subscribe(latestSubscriber);
        
        latestSubscriber.request(1); // Only process 1 batch, causing backpressure
        latestSubscriber.awaitDone(5, TimeUnit.SECONDS).assertNoErrors();
        
        // Extract and verify the exact batch numbers captured
        List<Integer> oldestBatchNums = extractBatchNumbers(oldestDropped);
        List<Integer> latestBatchNums = extractBatchNumbers(latestDropped);
        
        // Both strategies should capture some data
        Assertions.assertThat(oldestBatchNums)
                .as("DROP_OLDEST should capture some batch numbers")
                .isNotEmpty();
        Assertions.assertThat(latestBatchNums)
                .as("DROP_LATEST should capture some batch numbers")
                .isNotEmpty();
        
        // Verify the specific behavior:
        // DROP_OLDEST should capture early batch numbers (1, 2, 3, etc.)
        Assertions.assertThat(oldestBatchNums)
                .as("DROP_OLDEST should capture early batch numbers")
                .allMatch(batchNum -> batchNum <= 4);
        
        // DROP_LATEST should capture later batch numbers (3, 4, 5, 6)
        Assertions.assertThat(latestBatchNums)
                .as("DROP_LATEST should capture later batch numbers")
                .allMatch(batchNum -> batchNum >= 3);
        
        // The minimum batch number in DROP_OLDEST should be lower than in DROP_LATEST
        int minOldest = oldestBatchNums.stream().min(Integer::compareTo).orElse(999);
        int minLatest = latestBatchNums.stream().min(Integer::compareTo).orElse(0);
        
        Assertions.assertThat(minOldest)
                .as("DROP_OLDEST should capture lower batch numbers than DROP_LATEST")
                .isLessThan(minLatest);
        
        // Verify the captured data contains valid line protocol with expected batch numbers
        for (String point : oldestDropped) {
            Assertions.assertThat(point)
                    .as("Captured point should be valid line protocol")
                    .matches("sequential,batch=\\d+ value=\\d+ \\d+");
        }
        
        for (String point : latestDropped) {
            Assertions.assertThat(point)
                    .as("Captured point should be valid line protocol")
                    .matches("sequential,batch=\\d+ value=\\d+ \\d+");
        }
    }

    /**
     * Extract batch numbers from line protocol strings for verification
     */
    private List<Integer> extractBatchNumbers(List<String> lineProtocolPoints) {
        return lineProtocolPoints.stream()
                .map(line -> {
                    try {
                        int batchStart = line.indexOf("batch=") + 6;
                        if (batchStart == 5) return -1; // "batch=" not found
                        
                        int batchEnd = line.indexOf(" ", batchStart);
                        if (batchEnd == -1) batchEnd = line.length();
                        
                        String batchNumStr = line.substring(batchStart, batchEnd);
                        return Integer.parseInt(batchNumStr);
                    } catch (Exception e) {
                        return -1; // Invalid format
                    }
                })
                .filter(batchNum -> batchNum != -1)
                .distinct()
                .sorted()
                .collect(java.util.stream.Collectors.toList());
    }

    private TestSubscriber<AbstractWriteClient.BatchWriteItem> createTestSubscriber() {
        return new TestSubscriber<>(new DefaultSubscriber<AbstractWriteClient.BatchWriteItem>() {
            @Override
            protected void onStart() {}

            @Override
            public void onComplete() {}

            @Override
            public void onError(Throwable e) {}

            @Override
            public void onNext(AbstractWriteClient.BatchWriteItem t) {}
        }, 0L);
    }

    private Flowable<AbstractWriteClient.BatchWriteItem> createSequentialBatches(int batchCount) {
        return Flowable.unsafeCreate(s -> {
            BooleanSubscription bs = new BooleanSubscription();
            s.onSubscribe(bs);
            
            for (int batchNum = 1; batchNum <= batchCount && !bs.isCancelled(); batchNum++) {
                WriteParameters writeParameters = new WriteParameters("test-bucket", "test-org", WritePrecision.NS);
                AbstractWriteClient.BatchWriteDataGrouped data = new AbstractWriteClient.BatchWriteDataGrouped(writeParameters);
                
                // Single point per batch with clear batch identification
                data.append(String.format("sequential,batch=%d value=%d %d", batchNum, batchNum * 100, batchNum * 1000000L));
                
                s.onNext(new AbstractWriteClient.BatchWriteItem(writeParameters, data));
                
                // Small delay to ensure predictable ordering
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            
            if (!bs.isCancelled()) {
                s.onComplete();
            }
        });
    }
}