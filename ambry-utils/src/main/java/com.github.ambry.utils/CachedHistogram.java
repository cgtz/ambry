package com.github.ambry.utils;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.util.concurrent.TimeUnit;


/**
 * An extension of {@link Histogram} for situations where the value of a histogram at a specific quantile is desired.
 * Calling {@link Histogram#getSnapshot()} can be expensive when there is contention, so this wrapper stores a cached
 * value of the histogram quantile for the specified timeout instead of creating a new snapshot on every call to
 * {@link #getCachedValue()}.
 */
public class CachedHistogram extends Histogram {
  private final CachedGauge<Double> cache;

  /**
   * @param reservoir the {@link Reservoir} to use for the histogram.
   * @param timeoutMs the timeout for the value stored in the cache in milliseconds. After this time has passed, a new
   *                  value of the histogram at {@code quantile} will be calculated.
   * @param quantile the quantile of the histogram to cache.
   */
  public CachedHistogram(Reservoir reservoir, long timeoutMs, double quantile) {
    this(Clock.defaultClock(), reservoir, timeoutMs, quantile);
  }

  /**
   * Exposed for testing.
   * @param clock the {@link Clock} to use for the {@link CachedGauge}.
   * @param reservoir the {@link Reservoir} to use for the histogram.
   * @param timeoutMs the timeout for the value stored in the cache in milliseconds. After this time has passed, a new
   *                  value of the histogram at {@code quantile} will be calculated.
   * @param quantile the quantile of the histogram to cache.
   */
  CachedHistogram(Clock clock, Reservoir reservoir, long timeoutMs, double quantile) {
    super(reservoir);
    cache = new CachedGauge<Double>(clock, timeoutMs, TimeUnit.MILLISECONDS) {
      @Override
      protected Double loadValue() {
        return getSnapshot().getValue(quantile);
      }
    };
  }

  /**
   * @return the cached value of the histogram at the quantile specified via the constructor.
   */
  public double getCachedValue() {
    return cache.getValue();
  }
}
