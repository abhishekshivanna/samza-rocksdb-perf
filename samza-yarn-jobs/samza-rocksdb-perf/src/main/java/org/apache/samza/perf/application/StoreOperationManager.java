package org.apache.samza.perf.application;

import java.util.Arrays;
import java.util.List;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SamzaHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class StoreOperationManager {
  private static final Logger LOG = LoggerFactory.getLogger(StoreOperationManager.class);
  private static final String NUM_OPS = "numOps";
  private static final String GET_NS_PCT = "getNsPct";
  private static final String PUT_NS_PCT = "putNsPct";
  private static final String DELETE_NS_PCT = "deleteNsPct";
  private static final String ALL_NS_PCT = "allNsPct";
  private static final String RANGE_NS_PCT = "rangeNsPct";
  private static final String SNAPSHOT_NS_PCT = "snapshotNsPct";
  private static final String METRIC_GROUP = PerfTask.class.getName();
  private static final String NUM_RECORDS_BOOTSTRAPPED = "numRecordsBootstrapped";
  private static final String TIME_TO_BOOTSTRAP_MS = "timeToBootstrapMs";
  private static final List<Double> PERCENTILES = Arrays.asList(50D, 90D, 95D, 99D);

  private long bootstrapCount;
  private Gauge<Long> numRecordsbootstrapped;
  private Gauge<Long> timeToBootstrapMs;
  private Counter numOps;
  private SamzaHistogram getNsPct;
  private SamzaHistogram putNsPct;
  private SamzaHistogram deleteNsPct;
  private SamzaHistogram allNsPct;
  private SamzaHistogram rangeNsPct;
  private SamzaHistogram snapshotNsPct;
  private long bootstrapStartTime;
  private long bootstrapEndTime;

  StoreOperationManager(Context context) {
    MetricsRegistry containerRegistry = context.getContainerContext().getContainerMetricsRegistry();
    MetricsRegistry taskRegistry = context.getTaskContext().getTaskMetricsRegistry();
    numOps = taskRegistry.newCounter(METRIC_GROUP, NUM_OPS);
    numRecordsbootstrapped = taskRegistry.newGauge(METRIC_GROUP, NUM_RECORDS_BOOTSTRAPPED, 0L);
    timeToBootstrapMs = taskRegistry.newGauge(METRIC_GROUP, TIME_TO_BOOTSTRAP_MS, 0L);
    bootstrapCount = 0L;
    getNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, GET_NS_PCT);
    putNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, PUT_NS_PCT);
    deleteNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, DELETE_NS_PCT);
    allNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, ALL_NS_PCT);
    rangeNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, RANGE_NS_PCT);
    snapshotNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, SNAPSHOT_NS_PCT);
  }

  void incBootstrappedRecords(long items) {
    bootstrapCount += items;
    numRecordsbootstrapped.set(bootstrapCount);
  }

  void timedGet(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    getNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  void timedPut(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    putNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  void timedDelete(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    deleteNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  void timedAll(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    allNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  void timedRangeScan(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    rangeNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  void timedSnapshot(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    snapshotNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  void computePercentileMetrics() {
    LOG.info("Publishing percentile metrics");
    PERCENTILES.forEach(p -> {
      getNsPct.updateGaugeValues(p);
      putNsPct.updateGaugeValues(p);
      deleteNsPct.updateGaugeValues(p);
      allNsPct.updateGaugeValues(p);
      rangeNsPct.updateGaugeValues(p);
      snapshotNsPct.updateGaugeValues(p);
    });
  }

  void recordBootstrapStartTime() {
    bootstrapStartTime = System.currentTimeMillis();
  }

  void recordBootstrapEndTime() {
    bootstrapEndTime = System.currentTimeMillis();
    timeToBootstrapMs.set(bootstrapEndTime - bootstrapStartTime);
  }
}
