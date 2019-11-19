package com.github.ambry.utils;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test {@link CachedHistogram}.
 */
public class CachedHistogramTest {

  /**
   * Test caching behavior.
   */
  @Test
  public void testCache() {
    AtomicInteger snapshotCalls = new AtomicInteger(0);
    MockClock clock = new MockClock();
    Reservoir reservoir = new ExponentiallyDecayingReservoir();
    CachedHistogram histogram = new CachedHistogram(clock, reservoir, TimeUnit.SECONDS.toMillis(1), 0.50) {
      @Override
      public Snapshot getSnapshot() {
        // count number of calls to test caching
        snapshotCalls.getAndIncrement();
        return super.getSnapshot();
      }
    };
    long value = 2;
    double epsilon = 0.01;
    histogram.update(value);
    // getSnapshot should be called the first time
    assertEquals(value, histogram.getCachedValue(), epsilon);
    assertEquals(1, snapshotCalls.get());
    // the cached value should be used and getSnapshot should not be called.
    assertEquals(value, histogram.getCachedValue(), epsilon);
    assertEquals(1, snapshotCalls.get());
    // after progressing time, the cached value should expire and getSnapshot should be called
    clock.tick(1);
    assertEquals(value, histogram.getCachedValue(), epsilon);
    assertEquals(2, snapshotCalls.get());
  }
}
