/*
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.influxdb.client;

import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Jakub Bednar (bednar@github) (21/09/2018 10:35)
 */
class WriteOptionsTest {

    @Test
    void defaults() {

        WriteOptions writeOptions = WriteOptions.builder().build();

        Assertions.assertThat(writeOptions.getBatchSize()).isEqualTo(1000);
        Assertions.assertThat(writeOptions.getBufferLimit()).isEqualTo(10000);
        Assertions.assertThat(writeOptions.getFlushInterval()).isEqualTo(1000);
        Assertions.assertThat(writeOptions.getJitterInterval()).isEqualTo(0);
        Assertions.assertThat(writeOptions.getRetryInterval()).isEqualTo(5_000);
        Assertions.assertThat(writeOptions.getMaxRetries()).isEqualTo(5);
        Assertions.assertThat(writeOptions.getMaxRetryTime()).isEqualTo(180_000);
        Assertions.assertThat(writeOptions.getMaxRetryDelay()).isEqualTo(125_000);
        Assertions.assertThat(writeOptions.getExponentialBase()).isEqualTo(2);
        Assertions.assertThat(writeOptions.getConcatMapPrefetch()).isEqualTo(2);
        Assertions.assertThat(writeOptions.getWriteScheduler()).isEqualTo(Schedulers.newThread());
        Assertions.assertThat(writeOptions.getBackpressureStrategy()).isEqualTo(BackpressureOverflowStrategy.DROP_OLDEST);
        Assertions.assertThat(writeOptions.getCaptureBackpressureData()).isFalse();
    }

    @Test
    void configure() {

        WriteOptions writeOptions = WriteOptions.builder()
                .batchSize(10_000)
                .bufferLimit(500)
                .flushInterval(500)
                .jitterInterval(1_000)
                .retryInterval(2_000)
                .maxRetries(5)
                .maxRetryDelay(250_123)
                .exponentialBase(2)
                .concatMapPrefetch(5)
                .writeScheduler(Schedulers.computation())
                .backpressureStrategy(BackpressureOverflowStrategy.ERROR)
                .captureBackpressureData(true)
                .build();

        Assertions.assertThat(writeOptions.getBatchSize()).isEqualTo(10_000);
        Assertions.assertThat(writeOptions.getBufferLimit()).isEqualTo(500);
        Assertions.assertThat(writeOptions.getFlushInterval()).isEqualTo(500);
        Assertions.assertThat(writeOptions.getJitterInterval()).isEqualTo(1_000);
        Assertions.assertThat(writeOptions.getRetryInterval()).isEqualTo(2_000);
        Assertions.assertThat(writeOptions.getMaxRetries()).isEqualTo(5);
        Assertions.assertThat(writeOptions.getMaxRetryDelay()).isEqualTo(250_123);
        Assertions.assertThat(writeOptions.getExponentialBase()).isEqualTo(2);
        Assertions.assertThat(writeOptions.getConcatMapPrefetch()).isEqualTo(5);
        Assertions.assertThat(writeOptions.getWriteScheduler()).isEqualTo(Schedulers.computation());
        Assertions.assertThat(writeOptions.getBackpressureStrategy()).isEqualTo(BackpressureOverflowStrategy.ERROR);
        Assertions.assertThat(writeOptions.getCaptureBackpressureData()).isTrue();
    }

    @Test
    void concatMapPrefetchValidation() {
        // Test that concatMapPrefetch must be positive
        Assertions.assertThatThrownBy(() -> WriteOptions.builder().concatMapPrefetch(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concatMapPrefetch");

        Assertions.assertThatThrownBy(() -> WriteOptions.builder().concatMapPrefetch(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concatMapPrefetch");

        // Test that positive values work
        WriteOptions options1 = WriteOptions.builder().concatMapPrefetch(1).build();
        Assertions.assertThat(options1.getConcatMapPrefetch()).isEqualTo(1);

        WriteOptions options10 = WriteOptions.builder().concatMapPrefetch(10).build();
        Assertions.assertThat(options10.getConcatMapPrefetch()).isEqualTo(10);
    }

    @Test
    void captureBackpressureDataConfiguration() {
        // Test default value
        WriteOptions defaultOptions = WriteOptions.builder().build();
        Assertions.assertThat(defaultOptions.getCaptureBackpressureData()).isFalse();

        // Test explicit configuration
        WriteOptions enabledOptions = WriteOptions.builder()
                .captureBackpressureData(true)
                .build();
        Assertions.assertThat(enabledOptions.getCaptureBackpressureData()).isTrue();

        WriteOptions disabledOptions = WriteOptions.builder()
                .captureBackpressureData(false)
                .build();
        Assertions.assertThat(disabledOptions.getCaptureBackpressureData()).isFalse();
    }

    @Test
    void backpressureConfiguration() {
        // Test that backpressure options can be configured together
        WriteOptions options = WriteOptions.builder()
                .batchSize(100)
                .bufferLimit(500)
                .backpressureStrategy(BackpressureOverflowStrategy.DROP_LATEST)
                .captureBackpressureData(true)
                .build();

        Assertions.assertThat(options.getBatchSize()).isEqualTo(100);
        Assertions.assertThat(options.getBufferLimit()).isEqualTo(500);
        Assertions.assertThat(options.getBackpressureStrategy()).isEqualTo(BackpressureOverflowStrategy.DROP_LATEST);
        Assertions.assertThat(options.getCaptureBackpressureData()).isTrue();
    }

    @Test
    void backpressureStrategies() {
        // Test different backpressure strategies
        WriteOptions dropOldest = WriteOptions.builder()
                .backpressureStrategy(BackpressureOverflowStrategy.DROP_OLDEST)
                .build();
        Assertions.assertThat(dropOldest.getBackpressureStrategy()).isEqualTo(BackpressureOverflowStrategy.DROP_OLDEST);

        WriteOptions dropLatest = WriteOptions.builder()
                .backpressureStrategy(BackpressureOverflowStrategy.DROP_LATEST)
                .build();
        Assertions.assertThat(dropLatest.getBackpressureStrategy()).isEqualTo(BackpressureOverflowStrategy.DROP_LATEST);

        WriteOptions error = WriteOptions.builder()
                .backpressureStrategy(BackpressureOverflowStrategy.ERROR)
                .build();
        Assertions.assertThat(error.getBackpressureStrategy()).isEqualTo(BackpressureOverflowStrategy.ERROR);
    }
}