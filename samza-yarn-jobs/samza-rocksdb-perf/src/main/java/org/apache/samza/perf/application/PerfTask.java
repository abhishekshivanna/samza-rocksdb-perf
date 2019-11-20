package org.apache.samza.perf.application;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import java.nio.charset.StandardCharsets;
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
  private static final Double LOG_CADENCE = 0.2;
  private static final Random RANDOM = new Random(87324324);

  private int itemsToBootstrapPerPartition;
  private int itemsBootstrapped;
  private int itemsDeleted;
  private int valueSizeBytes;
  private int numKeyBuckets;
  private KeyValueStore<String, String> store;
  private String taskName;
  private boolean done;
  private long perTaskWindowMs;
  private int numTasks;
  private int bootstrapBatchSize;
  @SuppressWarnings("UnstableApiUsage")
  private TreeRangeMap<Double, StoreOperation> operationRangeMap;

  @Override
  public void init(Context context) throws Exception {
    jobConfig = new PerfJobConfig(new MapConfig(context.getJobContext().getConfig()));

    operationRangeMap = getStoreOperationRangeMap();
    bootstrapBatchSize = jobConfig.getBootstrapBatchSize();
    numKeyBuckets = jobConfig.getKeySpaceBuckets();
    valueSizeBytes = jobConfig.getStoreValueSizeBytes();
    taskName = context.getTaskContext().getTaskModel().getTaskName().getTaskName();
    numTasks = context.getContainerContext().getContainerModel().getTasks().entrySet().size();

    long storeSizeBytes = jobConfig.getPerTaskStoreSizeMb() * 1000000L;
    itemsToBootstrapPerPartition = new Long(storeSizeBytes / valueSizeBytes).intValue();
    itemsBootstrapped = 0;
    itemsDeleted = 0;
    store = (KeyValueStore<String, String>) context.getTaskContext().getStore(jobConfig.getStoreName());
    done = false;
    perTaskWindowMs = getPerTaskWindowExecutionDurationMs();

    LOG.info("Initialized PerfTask.");
  }

  @SuppressWarnings("UnstableApiUsage")
  private TreeRangeMap<Double, StoreOperation> getStoreOperationRangeMap() {
    TreeRangeMap<Double, StoreOperation> operationRangeMap = TreeRangeMap.create();
    double getRatio = jobConfig.getGetRatio();
    double putRatio = jobConfig.getPutRatio();
    double deleteRatio = jobConfig.getDeleteRatio();
    double allKeysScanRatio = jobConfig.getAllKeysScanRatio();
    double snapshotRatio = jobConfig.getSnapshotRatio();
    double rangeRatio = jobConfig.getRangeScanRatio();
    Preconditions.checkArgument(
        getRatio + putRatio + deleteRatio + allKeysScanRatio + snapshotRatio + rangeRatio == 1.0,
        "Fractions of workload for each store operation must sum to 1.0. But was: " + (getRatio + putRatio + deleteRatio
            + allKeysScanRatio + snapshotRatio + rangeRatio));

    Range<Double> getRange = Range.openClosed(0D, getRatio);
    Range<Double> putRange = Range.openClosed(getRange.upperEndpoint(), getRange.upperEndpoint() + putRatio);
    Range<Double> deleteRange = Range.openClosed(putRange.upperEndpoint(), putRange.upperEndpoint() + deleteRatio);
    Range<Double> allRange =
        Range.openClosed(deleteRange.upperEndpoint(), deleteRange.upperEndpoint() + allKeysScanRatio);
    Range<Double> snapshotRange = Range.openClosed(allRange.upperEndpoint(), allRange.upperEndpoint() + snapshotRatio);
    Range<Double> rangeRange =
        Range.openClosed(snapshotRange.upperEndpoint(), snapshotRange.upperEndpoint() + rangeRatio);

    operationRangeMap.put(getRange, GET);
    operationRangeMap.put(putRange, PUT);
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
    LOG.info("from " + taskName);
    long startTime = System.currentTimeMillis();
    if (done) {
      return;
    }

    // iterate until at least perTaskWindowMs has elapsed
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
          case PUT:
            performPut();
            break;
          case DELETE:
            performDelete();
            break;
          case ALL:
            performFullRangeScan();
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
  }

  private boolean storeIsEmpty() {
    return itemsDeleted == itemsBootstrapped;
  }

  private void performRangeScan() {
    Pair<String, String> pair = pickRandomFromTo();
    KeyValueIterator<String, String> rangeIterator = store.range(pair.getLeft(), pair.getRight());
    iterateAndClose(rangeIterator);
  }

  private void performSnapshot() {
    Pair<String, String> pair = pickRandomFromTo();
    KeyValueSnapshot<String, String> snapshot = store.snapshot(pair.getLeft(), pair.getRight());
    iterateAndClose(snapshot.iterator());
    snapshot.close();
  }

  private void performFullRangeScan() {
    KeyValueIterator<String, String> allIterator = store.all();
    iterateAndClose(allIterator);
  }

  private void performDelete() {
    String keyToDelete = pickKeyToDelete();
    store.delete(keyToDelete);
    itemsDeleted++;
    if (shouldLog(itemsDeleted)) {
      LOG.info("{} records deleted from partition {}", itemsDeleted, taskName);
    }
  }

  private void performPut() {
    String keyToOverwrite = pickRandomKeyInTable();
    store.put(keyToOverwrite, generateRandomString(valueSizeBytes));
  }

  private void performGet() {
    String keyToGet = pickRandomKeyInTable();
    store.get(keyToGet);
  }

  private void performBootstrap() {
    if (bootstrapBatchSize == 1) {
      store.put(Long.toString(itemsBootstrapped), generateRandomString(valueSizeBytes));
    } else {
      List<Entry<String, String>> entries = IntStream.range(itemsBootstrapped, itemsBootstrapped + bootstrapBatchSize)
          .boxed()
          .map(i -> new Entry<>(Long.toString(i), generateRandomString(valueSizeBytes)))
          .collect(Collectors.toList());
      store.putAll(entries);
    }
    itemsBootstrapped += bootstrapBatchSize;
    if (shouldLog(itemsBootstrapped)) {
      LOG.info("{} records inserted in {}", itemsBootstrapped, taskName);
    }
  }

  /**
   * Selects a random key that exists in the table. This is computed by first randomly selecting a non-empty bucket,
   * and then selecting a non-deleted key in that bucket.
   */
  private String pickRandomKeyInTable() {
    int emptyBuckets = Math.max(0, itemsDeleted - (itemsToBootstrapPerPartition - numKeyBuckets));
    int selectedBucket = RANDOM.nextInt(numKeyBuckets - emptyBuckets) + emptyBuckets;
    int numDeletedFromBucket =
        selectedBucket < getNextDeletionBucket() ? getNextDeletionIndex() + 1 : getNextDeletionIndex();
    int selectedIndex = RANDOM.nextInt(getBucketSize() - numDeletedFromBucket) + numDeletedFromBucket;
    int selectedKey = selectedBucket * getBucketSize() + selectedIndex;

    return Integer.toString(selectedKey);
  }

  /**
   * Selects the next key that exists to delete from the table.
   *
   * Key selection is designed to round robin across buckets, to simulate more random rather than sequential deletes.
   * With a key space of 50 divided into 5 buckets, keys would be generated in the following order:
   *
   * 0, 10, 20, 30, 40,
   * 1, 11, 21, 31, 41,
   * ...
   * 9, 19, 29, 39, 49
   *
   * Where each column above represents a bucket of keys.
   */
  private String pickKeyToDelete() {
    int bucketToDeleteFrom = getNextDeletionBucket();
    int indexToDelete = getNextDeletionIndex();
    int keyToDelete = bucketToDeleteFrom * getBucketSize() + indexToDelete;

    return Long.toString(keyToDelete);
  }

  /**
   * Returns whether to log for the given record.
   */
  private boolean shouldLog(int recordNum) {
    return recordNum % (itemsToBootstrapPerPartition * LOG_CADENCE) == 0;
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

  /**
   * The bucket to be deleted from next.
   */
  private int getNextDeletionBucket() {
    return itemsDeleted % numKeyBuckets;
  }

  /**
   * The next index (in the next bucket) to delete.
   */
  private int getNextDeletionIndex() {
    return itemsDeleted / numKeyBuckets;
  }

  /**
   * The number of keys in each bucket.
   */
  private int getBucketSize() {
    return itemsToBootstrapPerPartition / numKeyBuckets;
  }

  /*
   * Total time each tasks should execute during a window
   * in order to prevent other tasks from starvation
   * */
  private int getPerTaskWindowExecutionDurationMs() {
    int threadpoolCount = jobConfig.getContainerThreadpoolCount();
    if (threadpoolCount == 1) {
      return (int) (jobConfig.getWindowMs() / numTasks);
    } else {
      return (int) (jobConfig.getWindowMs() / Math.ceil((double) numTasks / (double) threadpoolCount));
    }
  }
}
