package org.apache.samza.perf.application;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.perf.config.PerfJobConfig;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueSnapshot;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.perf.application.StoreOperation.*;


public class PerfTask implements StreamTask, InitableTask, WindowableTask {

  private PerfJobConfig jobConfig;
  private static final Logger LOG = LoggerFactory.getLogger(PerfTask.class);
  private static final double LOG_CADENCE = 0.2;

  private static final Random RANDOM = new Random(123456);

  private int itemsToBootstrapPerPartition;
  private int itemsBootstrapped;
  private int itemsDeleted;
  private int valueSizeBytes;
  private long percentileComputeWindowSeconds;
  private KeyValueStore<String, String> store;
  private String taskName;
  private boolean done;
  private long perTaskWindowMs;
  private int numTasks;
  private int bootstrapBatchSize;
  private long nextPctComputeTimeMs;
  @SuppressWarnings("UnstableApiUsage")
  private TreeRangeMap<Double, StoreOperation> operationRangeMap;
  private StoreOperationManager storeOperationManager;
  private BitSet insertedSet;
  private BitSet deletedSet;

  @Override
  public void init(Context context) throws Exception {
    jobConfig = new PerfJobConfig(new MapConfig(context.getJobContext().getConfig()));
    storeOperationManager = new StoreOperationManager(context);
    operationRangeMap = getStoreOperationRangeMap();
    percentileComputeWindowSeconds = jobConfig.getPercentileMetricsComputeWindowSeconds();
    bootstrapBatchSize = jobConfig.getBootstrapBatchSize();
    valueSizeBytes = jobConfig.getStoreValueSizeBytes();
    taskName = context.getTaskContext().getTaskModel().getTaskName().getTaskName();
    numTasks = context.getContainerContext().getContainerModel().getTasks().entrySet().size();
    nextPctComputeTimeMs = System.currentTimeMillis();

    long storeSizeBytes = jobConfig.getPerTaskStoreSizeMb() * 1024L * 1024L;
    int keySizeBytes = jobConfig.getStoreKeySizeBytes();
    itemsToBootstrapPerPartition = new Long(storeSizeBytes / (valueSizeBytes + keySizeBytes)).intValue();
    itemsBootstrapped = 0;
    itemsDeleted = 0;
    store = (KeyValueStore<String, String>) context.getTaskContext().getStore(jobConfig.getStoreName());
    done = false;
    perTaskWindowMs = getPerTaskWindowExecutionDurationMs();

    insertedSet = new BitSet();
    deletedSet = new BitSet();

    LOG.info("Initialized PerfTask.");
    LOG.info(
        "Task[{}] will bootstrap {} records [keySize: {}, valueSize: {}] into the store before performing workload",
        taskName, itemsToBootstrapPerPartition, keySizeBytes, valueSizeBytes);
  }

  @SuppressWarnings("UnstableApiUsage")
  private TreeRangeMap<Double, StoreOperation> getStoreOperationRangeMap() {
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

  /**
   * No-op.
   */
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (done) {
      return;
    }
    // iterate until at least perTaskWindowMs has elapsed
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime <= perTaskWindowMs) {
      if (itemsBootstrapped < itemsToBootstrapPerPartition) {
        performBootstrap();
      } else {
        // Determine the operation to do based on the range the random number falls in
        Double random = RANDOM.nextDouble();
        StoreOperation op = operationRangeMap.get(random);
        switch (op) {
          case GET:
            performGet();
            break;
          case INSERT:
            performInsert();
            break;
          case UPDATE:
            performUpdate();
            break;
          case DELETE:
            performDelete();
            break;
          case ALL:
            performAll();
            break;
          case RANGE:
            performRangeScan();
            break;
          case SNAPSHOT:
            performSnapshot();
            break;
          default:
            throw new IllegalStateException("Unexpected value: " + random);
        }
      }
      if (storeIsEmpty()) {
        LOG.info("{} store now empty; task will idle", taskName);
        done = true;
        break;
      }
    }
    if (nextPctComputeTimeMs <= startTime) { // cheaper to compare with startTime instead of System.currentTimeMillis()
      storeOperationManager.computePercentileMetrics();
      nextPctComputeTimeMs += Duration.ofSeconds(percentileComputeWindowSeconds).toMillis();
    }
  }

  private boolean storeIsEmpty() {
    return itemsDeleted == itemsBootstrapped;
  }

  private void performBootstrap() {
    if (itemsBootstrapped == 0) {
      LOG.info("Starting RocksDB bootstrap");
      storeOperationManager.recordBootstrapStartTime();
    }
    if (bootstrapBatchSize == 1) {
      String randomValue = generateRandomString(valueSizeBytes);
      insertedSet.set(itemsBootstrapped);
      storeOperationManager.timedPut(() -> store.put(Long.toString(itemsBootstrapped), randomValue));
    } else {
      List<Entry<String, String>> entries =
          IntStream.range(itemsBootstrapped, itemsBootstrapped + bootstrapBatchSize).boxed().map(i -> {
            insertedSet.set(i);
            return new Entry<>(Long.toString(i), generateRandomString(valueSizeBytes));
          }).collect(Collectors.toList());

      storeOperationManager.timedPut(() -> store.putAll(entries));
    }
    itemsBootstrapped += bootstrapBatchSize;
    storeOperationManager.incBootstrappedRecords(bootstrapBatchSize);
    if (itemsBootstrapped >= itemsToBootstrapPerPartition) {
      storeOperationManager.recordBootstrapEndTime();
      LOG.info("Bootstrap done");
    }
    if (shouldLog(itemsBootstrapped)) {
      LOG.info("{} records inserted in {}", itemsBootstrapped, taskName);
    }
  }

  private void performGet() {
    String keyToGet = Integer.toString(pickKeyInTable());
    storeOperationManager.timedGet(() -> store.get(keyToGet));
  }

  private void performInsert() {
    int key = pickKeyNotInTable();
    if (key == -1) {
      // if nothing new to insert perform Update
      performUpdate();
      return;
    }
    String randomValue = generateRandomString(valueSizeBytes);
    storeOperationManager.timedPut(() -> store.put(Integer.toString(key), randomValue));
    deletedSet.clear(key);
    insertedSet.set(key);
  }

  private void performUpdate() {
    int key = pickKeyInTable();
    String randomValue = generateRandomString(valueSizeBytes);
    storeOperationManager.timedPut(() -> store.put(Integer.toString(key), randomValue));
    deletedSet.clear(key);
    insertedSet.set(key);
  }

  private void performRangeScan() {
    Pair<String, String> pair = pickRandomFromTo();
    storeOperationManager.timedRangeScan(() -> {
      KeyValueIterator<String, String> rangeIterator = store.range(pair.getLeft(), pair.getRight());
      iterateAndClose(rangeIterator);
    });
  }

  private void performSnapshot() {
    Pair<String, String> pair = pickRandomFromTo();
    storeOperationManager.timedSnapshot(() -> {
      KeyValueSnapshot<String, String> snapshot = store.snapshot(pair.getLeft(), pair.getRight());
      iterateAndClose(snapshot.iterator());
      snapshot.close();
    });
  }

  private void performAll() {
    storeOperationManager.timedAll(() -> {
      KeyValueIterator<String, String> allIterator = store.all();
      iterateAndClose(allIterator);
    });
  }

  private void performDelete() {
    int key = pickKeyInTable();
    storeOperationManager.timedDelete(() -> store.delete(Integer.toString(key)));
    deletedSet.set(key);
    insertedSet.clear(key);
    itemsDeleted++;
    if (shouldLog(itemsDeleted)) {
      LOG.info("{} records deleted from partition {}", itemsDeleted, taskName);
    }
  }

  private int pickKeyNotInTable() {
    int index = 0;
    int selected = 0;
    if (deletedSet.cardinality() == 0) {
      return -1;
    }
    do {
      index = RANDOM.nextInt(itemsBootstrapped);
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
      index = RANDOM.nextInt(itemsBootstrapped);
      selected = insertedSet.nextSetBit(index);
    } while (selected == -1);
    return selected;
  }

  /**
   * Returns whether to log for the given record.
   */
  private boolean shouldLog(int recordNum) {
    return Math.floor(recordNum % (itemsToBootstrapPerPartition * LOG_CADENCE)) == 0;
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
    int n1 = RANDOM.nextInt(itemsBootstrapped);
    int n2 = RANDOM.nextInt(itemsBootstrapped);

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

  /*
   * Total time each tasks should execute during a window
   * in order to prevent other tasks from starvation
   * */
  private long getPerTaskWindowExecutionDurationMs() {
    int threadpoolCount = jobConfig.getContainerThreadpoolCount();
    if (threadpoolCount == 1) {
      if (jobConfig.getWindowMs() % numTasks == 0) {
        int bufferTimeBetweenWindowMs = 3;
        return (jobConfig.getWindowMs() / numTasks) - bufferTimeBetweenWindowMs;
      }
      return jobConfig.getWindowMs() / numTasks;
    } else {
      return (int) (jobConfig.getWindowMs() / Math.ceil((double) numTasks / (double) threadpoolCount));
    }
  }
}
