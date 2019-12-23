package org.apache.samza.perf.application;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SamzaHistogram;
import org.apache.samza.perf.application.simpleworkload.PerfTask;
import org.apache.samza.perf.config.PerfJobConfig;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueSnapshot;
import org.apache.samza.storage.kv.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.perf.application.StoreOperation.*;


public class StoreOperationManager {
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
  private static final Random RANDOM = new Random(123456);
  private static final double LOG_CADENCE = 0.2;
  private final boolean updatesAreSequential;
  private final int targetItemsPerPartition;
  private final int valueSizeBytes;
  private final int bootstrapBatchSize;
  private final String taskName;

  private boolean bootstrapPending;
  private int currentSequentialUpdateKey;
  private KeyValueStore<String, String> store;
  private int currentItemsPerPartition;
  private BitSet insertedSet;
  private BitSet deletedSet;

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
  @SuppressWarnings("UnstableApiUsage")
  private TreeRangeMap<Double, StoreOperation> operationRangeMap;

  public StoreOperationManager(Context context, KeyValueStore<String, String> kvstore) {

    /* Configs */
    PerfJobConfig jobConfig = new PerfJobConfig(new MapConfig(context.getJobContext().getConfig()));
    taskName = context.getTaskContext().getTaskModel().getTaskName().getTaskName();
    store = kvstore;

    /* OPs */
    long storeSizeBytes = jobConfig.getPerTaskStoreSizeMb() * 1024L * 1024L;
    int keySizeBytes = jobConfig.getStoreKeySizeBytes();
    boolean bootstrapEnabled = jobConfig.isBootstrapEnabled();

    currentSequentialUpdateKey = 0;
    updatesAreSequential = jobConfig.getUpdatesAreSequential();
    valueSizeBytes = jobConfig.getStoreValueSizeBytes();
    operationRangeMap = getStoreOperationRangeMap(jobConfig);
    bootstrapBatchSize = jobConfig.getBootstrapBatchSize();
    targetItemsPerPartition = new Long(storeSizeBytes / (valueSizeBytes + keySizeBytes)).intValue();
    currentItemsPerPartition = 0;
    insertedSet = new BitSet();
    deletedSet = new BitSet();
    bootstrapPending = bootstrapEnabled;
    if (!bootstrapEnabled) {
      currentItemsPerPartition = targetItemsPerPartition;
      insertedSet.set(0, targetItemsPerPartition);
    }

    /* Metrics */
    MetricsRegistry containerRegistry = context.getContainerContext().getContainerMetricsRegistry();
    MetricsRegistry taskRegistry = context.getTaskContext().getTaskMetricsRegistry();
    numOps = taskRegistry.newCounter(METRIC_GROUP, NUM_OPS);
    numRecordsbootstrapped = taskRegistry.newGauge(METRIC_GROUP, NUM_RECORDS_BOOTSTRAPPED, 0L);
    timeToBootstrapMs = taskRegistry.newGauge(METRIC_GROUP, TIME_TO_BOOTSTRAP_MS, 0L);
    bootstrapCount = 0L;
    getNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, GET_NS_PCT, PERCENTILES);
    putNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, PUT_NS_PCT, PERCENTILES);
    deleteNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, DELETE_NS_PCT, PERCENTILES);
    allNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, ALL_NS_PCT, PERCENTILES);
    rangeNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, RANGE_NS_PCT, PERCENTILES);
    snapshotNsPct = new SamzaHistogram(containerRegistry, METRIC_GROUP, SNAPSHOT_NS_PCT, PERCENTILES);
  }

  public StoreOperation getNextOperation() {
    if (bootstrapPending) {
      return BOOTSTRAP;
    }
    if (currentItemsPerPartition == 0) {
      return CLOSE;
    }
    Double random = RANDOM.nextDouble();
    return operationRangeMap.get(random);
  }

  public void performBootstrap() {
    if (currentItemsPerPartition == 0) {
      LOG.info("Starting RocksDB bootstrap");
      this.recordBootstrapStartTime();
    }
    if (bootstrapBatchSize == 1) {
      String randomValue = this.generateRandomString(valueSizeBytes);
      recordItemInserted(currentItemsPerPartition);
      timedPut(() -> store.put(Long.toString(currentItemsPerPartition), randomValue));
    } else {
      List<Entry<String, String>> entries =
          IntStream.range(currentItemsPerPartition, currentItemsPerPartition + bootstrapBatchSize).boxed().map(i -> {
            insertedSet.set(i);
            return new Entry<>(Long.toString(i), generateRandomString(valueSizeBytes));
          }).collect(Collectors.toList());

      timedPut(() -> store.putAll(entries));
    }
    currentItemsPerPartition += bootstrapBatchSize;
    incBootstrappedRecords(bootstrapBatchSize);
    if (currentItemsPerPartition >= targetItemsPerPartition) {
      recordBootstrapEndTime();
      bootstrapPending = false;
      LOG.info("Bootstrap done");
    }
    if (shouldLog(currentItemsPerPartition)) {
      LOG.info("{} records inserted into store {} in Task {}", currentItemsPerPartition, store, taskName);
    }
  }

  public void performGet() {
    String keyToGet = Integer.toString(pickKeyInTable());
    this.timedGet(() -> store.get(keyToGet));
  }

  public void performInsert() {
    int key = pickKeyNotInTable();
    if (key == -1) {
      // if nothing new to insert perform Update
      performUpdate();
      return;
    }
    String randomValue = generateRandomString(valueSizeBytes);
    this.timedPut(() -> store.put(Integer.toString(key), randomValue));
    recordItemInserted(key);
  }

  public void performUpdate() {
    int key = pickKeyForUpdate();
    String randomValue = generateRandomString(valueSizeBytes);
    this.timedPut(() -> store.put(Integer.toString(key), randomValue));
    recordItemUpdated(key);
  }

  private int pickKeyForUpdate() {
    if (!updatesAreSequential) {
      return pickKeyInTable();
    }
    currentSequentialUpdateKey = currentSequentialUpdateKey++ % targetItemsPerPartition;
    return currentSequentialUpdateKey;
  }

  public void performRangeScan() {
    Pair<String, String> pair = pickRandomFromTo();
    this.timedRangeScan(() -> {
      KeyValueIterator<String, String> rangeIterator = store.range(pair.getLeft(), pair.getRight());
      iterateAndClose(rangeIterator);
    });
  }

  public void performSnapshot() {
    Pair<String, String> pair = pickRandomFromTo();
    this.timedSnapshot(() -> {
      KeyValueSnapshot<String, String> snapshot = store.snapshot(pair.getLeft(), pair.getRight());
      iterateAndClose(snapshot.iterator());
      snapshot.close();
    });
  }

  public void performAll() {
    this.timedAll(() -> {
      KeyValueIterator<String, String> allIterator = store.all();
      iterateAndClose(allIterator);
    });
  }

  public void performDelete() {
    int key = pickKeyInTable();
    this.timedDelete(() -> store.delete(Integer.toString(key)));
    recordItemDeleted(key);
  }

  @SuppressWarnings("UnstableApiUsage")
  private TreeRangeMap<Double, StoreOperation> getStoreOperationRangeMap(PerfJobConfig jobConfig) {
    TreeRangeMap<Double, StoreOperation> operationRangeMap = TreeRangeMap.create();
    double getRatio = jobConfig.getGetRatio();
    double insertRatio = jobConfig.getInsertRatio();
    double updateRatio = jobConfig.getUpdateRatio();
    double deleteRatio = jobConfig.getDeleteRatio();
    double allKeysScanRatio = jobConfig.getAllKeysScanRatio();
    double snapshotRatio = jobConfig.getSnapshotRatio();
    double rangeRatio = jobConfig.getRangeScanRatio();
    Preconditions.checkArgument(
        getRatio + insertRatio + updateRatio + deleteRatio + allKeysScanRatio + snapshotRatio + rangeRatio == 1.0,
        "Fractions of workload for each store operation must sum to 1.0. But was: " + (getRatio + insertRatio
            + updateRatio + deleteRatio + allKeysScanRatio + snapshotRatio + rangeRatio));

    Range<Double> getRange = Range.openClosed(0D, getRatio);
    Range<Double> insertRange = Range.openClosed(getRange.upperEndpoint(), getRange.upperEndpoint() + insertRatio);
    Range<Double> updateRange =
        Range.openClosed(insertRange.upperEndpoint(), insertRange.upperEndpoint() + updateRatio);
    Range<Double> deleteRange =
        Range.openClosed(updateRange.upperEndpoint(), updateRange.upperEndpoint() + deleteRatio);
    Range<Double> allRange =
        Range.openClosed(deleteRange.upperEndpoint(), deleteRange.upperEndpoint() + allKeysScanRatio);
    Range<Double> snapshotRange = Range.openClosed(allRange.upperEndpoint(), allRange.upperEndpoint() + snapshotRatio);
    Range<Double> rangeRange =
        Range.openClosed(snapshotRange.upperEndpoint(), snapshotRange.upperEndpoint() + rangeRatio);

    operationRangeMap.put(getRange, GET);
    operationRangeMap.put(insertRange, INSERT);
    operationRangeMap.put(updateRange, UPDATE);
    operationRangeMap.put(deleteRange, DELETE);
    operationRangeMap.put(allRange, ALL);
    operationRangeMap.put(snapshotRange, SNAPSHOT);
    operationRangeMap.put(rangeRange, RANGE);
    return operationRangeMap;
  }

  private void incBootstrappedRecords(long items) {
    bootstrapCount += items;
    numRecordsbootstrapped.set(bootstrapCount);
  }

  private void timedGet(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    getNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  private void timedPut(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    putNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  private void timedDelete(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    deleteNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  private void timedAll(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    allNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  private void timedRangeScan(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    rangeNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  private void timedSnapshot(Runnable op) {
    long startTime = System.nanoTime();
    op.run();
    snapshotNsPct.update(System.nanoTime() - startTime);
    numOps.inc();
  }

  public void computePercentileMetrics() {
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

  private void recordBootstrapStartTime() {
    bootstrapStartTime = System.currentTimeMillis();
  }

  private void recordBootstrapEndTime() {
    long bootstrapEndTime = System.currentTimeMillis();
    timeToBootstrapMs.set(bootstrapEndTime - bootstrapStartTime);
  }

  private int pickKeyNotInTable() {
    int index = 0;
    int selected = 0;
    if (deletedSet.cardinality() == 0) {
      return -1;
    }
    do {
      index = RANDOM.nextInt(targetItemsPerPartition);
      selected = deletedSet.nextSetBit(index);
    } while (selected == -1);
    return selected;
  }

  private int pickKeyInTable() {
    int selected;
    int index;
    if (insertedSet.cardinality() == 0) {
      return -1;
    }
    do {
      index = RANDOM.nextInt(targetItemsPerPartition);
      selected = insertedSet.nextSetBit(index);
    } while (selected == -1);
    return selected;
  }

  /**
   * Generate a string of the given size filled with random bytes.
   */
  private String generateRandomString(int size) {
    byte[] byteArray = new byte[size];
    RANDOM.nextBytes(byteArray);

    return new String(byteArray, StandardCharsets.UTF_8);
  }

  /**
   * Selects a random (from, to) pair of keys where from <= to.
   *
   * Note: there is no guarantee about the existence of keys at or between from and to.
   */
  private Pair<String, String> pickRandomFromTo() {
    int n1 = RANDOM.nextInt(targetItemsPerPartition);
    int n2 = RANDOM.nextInt(targetItemsPerPartition);

    int from = Math.min(n1, n2);
    int to = Math.max(n1, n2);
    return new ImmutablePair<>(Integer.toString(from), Integer.toString(to));
  }

  /**
   * Iterates through all entries in the iterator, then closes it.
   */
  private void iterateAndClose(KeyValueIterator iter) {
    while (iter.hasNext()) {
      iter.next();
    }
    iter.close();
  }

  private void recordItemInserted(int item) {
    insertedSet.set(item);
    deletedSet.clear(item);
    currentItemsPerPartition++;
  }

  private void recordItemDeleted(int item) {
    deletedSet.set(item);
    insertedSet.clear(item);
    currentItemsPerPartition--;
  }

  private void recordItemUpdated(int item) {
    insertedSet.set(item);
    deletedSet.clear(item);
  }

  /**
   * Returns whether to log for the given record.
   */
  private boolean shouldLog(int recordNum) {
    return Math.floor(recordNum % (targetItemsPerPartition * LOG_CADENCE)) == 0;
  }
}
