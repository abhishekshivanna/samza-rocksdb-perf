package org.apache.samza.perf.config;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.List;
import org.apache.samza.config.MapConfig;


public class PerfJobConfig extends MapConfig {

  private static final String PERF_JOB_STORE_KEY_SIZE_BYTES = "perf.job.store.key.size.bytes";
  private static final int DEFAULT_STORE_KEY_SIZE_BYTES = 20;

  private static final String PERF_JOB_STORE_VALUE_SIZE_BYTES = "perf.job.store.value.size.bytes";
  private static final int DEFAULT_STORE_VALUE_SIZE_BYTES = 1000;

  private static final String STORE_NAME = "perf.job.store.name";
  private static final String DEFAULT_STORE_NAME = "perf-test-store";

  private static final String KAFKA_BOOSTRAP_SERVERS_KEY = "perf.job.kafka.bootstrap.servers";
  private static final ImmutableList<String> DEFAULT_KAFKA_BOOSTRAP_SERVERS_KEY = ImmutableList.of("localhost:9092");

  private static final String CONSUMER_ZK_CONNECT_KEY = "perf.job.kafka.consumer.zk.connect";
  private static final ImmutableList<String> DEFAULT_CONSUMER_ZK_CONNECT_KEY = ImmutableList.of("localhost:2181");

  private static final String PERF_JOB_TASK_STORE_SIZE_MB = "perf.job.task.store.size.mb";
  private static final int DEFAULT_TASK_STORE_SIZE_MB = 5;

  private static final String PERF_JOB_STORE_GET_RATIO = "perf.job.store.get.ratio";
  private static final double DEFAULT_PERF_JOB_STORE_GET_RATIO = 0.75;

  private static final String PERF_JOB_STORE_INSERT_RATIO = "perf.job.store.insert.ratio";
  private static final double DEFAULT_PERF_JOB_STORE_INSERT_RATIO = 0.0;

  private static final String PERF_JOB_STORE_UPDATE_RATIO = "perf.job.store.update.ratio";
  private static final double DEFAULT_PERF_JOB_STORE_UPDATE_RATIO = 0.25;

  private static final String PERF_JOB_STORE_DELETE_RATIO = "perf.job.store.delete.ratio";
  private static final double DEFAULT_PERF_JOB_STORE_DELETE_RATIO = 0.0;

  private static final String PERF_JOB_STORE_ALL_RANGE_SCAN_RATIO = "perf.job.store.all.range.scan.ratio";
  private static final double DEFAULT_PERF_JOB_STORE_ALL_RANGE_SCAN_RATIO = 0.0;

  private static final String PERF_JOB_STORE_SNAPSHOT_RATIO = "perf.job.store.snapshot.ratio";
  private static final double DEFAULT_PERF_JOB_STORE_SNAPSHOT_RATIO = 0.0;

  private static final String PERF_JOB_STORE_RANGE_SCAN_RATIO = "perf.job.store.range.scan.ratio";
  private static final double DEFAULT_PERF_JOB_STORE_RANGE_SCAN_RATIO = 0.0;

  private static final String TASK_WINDOW_MS = "task.window.ms";
  private static final int DEFAULT_TASK_WINDOW_MS = 1000;

  private static final String JOB_CONTAINER_THREAD_POOL_SIZE = "job.container.thread.pool.size";
  private static final int DEFAULT_JOB_CONTAINER_THREAD_POOL_SIZE = 1;

  private static final String PERF_JOB_BOOTSTRAP_BATCH_SIZE = "perf.job.bootstrap.batch.size";
  private static final int DEFAULT_PERF_JOB_BOOTSTRAP_BATCH_SIZE = 1;

  private static final String PERF_JOB_BOOTSTRAP_ENABLED = "perf.job.bootstrap.enabled";
  private static final boolean DEFAULT_PERF_JOB_BOOTSTRAP_ENABLED = true;

  private static final String PERF_JOB_INPUT_KAFKA_SYSTEM_NAME = "perf.job.input.kafka.system.name";
  private static final String DEFAULT_PERF_JOB_INPUT_KAFKA_SYSTEM_NAME = "queuing";

  private static final String PERF_JOB_INPUT_KAFKA_STREAM_ID = "perf.job.input.kafka.stream.id";
  private static final String DEFAULT_PERF_JOB_INPUT_KAFKA_STREAM_ID = "samza-perf-test-input-p1";

  private static final String PERF_JOB_PERCENTILE_METRICS_COMPUTE_WINDOW_SECONDS =
      "perf.job.percentile.metrics.compute.window.seconds";
  private static final long DEFAULT_PERF_JOB_PERCENTILE_METRICS_COMPUTE_WINDOW_SECONDS =
      Duration.ofMinutes(15).getSeconds();

  private MapConfig config;

  public PerfJobConfig(MapConfig config) {
    this.config = config;
  }

  public List<String> getKafkaBootstrapServers() {
    return config.getList(KAFKA_BOOSTRAP_SERVERS_KEY, DEFAULT_KAFKA_BOOSTRAP_SERVERS_KEY);
  }

  public List<String> getConsumerZkConnect() {
    return config.getList(CONSUMER_ZK_CONNECT_KEY, DEFAULT_CONSUMER_ZK_CONNECT_KEY);
  }

  public String getStoreName() {
    return config.get(STORE_NAME, DEFAULT_STORE_NAME);
  }

  public int getStoreKeySizeBytes() {
    return config.getInt(PERF_JOB_STORE_KEY_SIZE_BYTES, DEFAULT_STORE_KEY_SIZE_BYTES);
  }

  public long getPerTaskStoreSizeMb() {
    return config.getInt(PERF_JOB_TASK_STORE_SIZE_MB, DEFAULT_TASK_STORE_SIZE_MB);
  }

  public int getStoreValueSizeBytes() {
    return config.getInt(PERF_JOB_STORE_VALUE_SIZE_BYTES, DEFAULT_STORE_VALUE_SIZE_BYTES);
  }

  public double getGetRatio() {
    return config.getDouble(PERF_JOB_STORE_GET_RATIO, DEFAULT_PERF_JOB_STORE_GET_RATIO);
  }

  public double getInsertRatio() {
    return config.getDouble(PERF_JOB_STORE_INSERT_RATIO, DEFAULT_PERF_JOB_STORE_INSERT_RATIO);
  }

  public double getUpdateRatio() {
    return config.getDouble(PERF_JOB_STORE_UPDATE_RATIO, DEFAULT_PERF_JOB_STORE_UPDATE_RATIO);
  }

  public double getDeleteRatio() {
    return config.getDouble(PERF_JOB_STORE_DELETE_RATIO, DEFAULT_PERF_JOB_STORE_DELETE_RATIO);
  }

  public double getAllKeysScanRatio() {
    return config.getDouble(PERF_JOB_STORE_ALL_RANGE_SCAN_RATIO, DEFAULT_PERF_JOB_STORE_ALL_RANGE_SCAN_RATIO);
  }

  public double getSnapshotRatio() {
    return config.getDouble(PERF_JOB_STORE_SNAPSHOT_RATIO, DEFAULT_PERF_JOB_STORE_SNAPSHOT_RATIO);
  }

  public double getRangeScanRatio() {
    return config.getDouble(PERF_JOB_STORE_RANGE_SCAN_RATIO, DEFAULT_PERF_JOB_STORE_RANGE_SCAN_RATIO);
  }

  public long getWindowMs() {
    return config.getLong(TASK_WINDOW_MS, DEFAULT_TASK_WINDOW_MS);
  }

  public int getContainerThreadpoolCount() {
    return config.getInt(JOB_CONTAINER_THREAD_POOL_SIZE, DEFAULT_JOB_CONTAINER_THREAD_POOL_SIZE);
  }

  public int getBootstrapBatchSize() {
    return config.getInt(PERF_JOB_BOOTSTRAP_BATCH_SIZE, DEFAULT_PERF_JOB_BOOTSTRAP_BATCH_SIZE);
  }

  public String getInputSystemName() {
    return config.get(PERF_JOB_INPUT_KAFKA_SYSTEM_NAME, DEFAULT_PERF_JOB_INPUT_KAFKA_SYSTEM_NAME);
  }

  public String getInputStreamId() {
    return config.get(PERF_JOB_INPUT_KAFKA_STREAM_ID, DEFAULT_PERF_JOB_INPUT_KAFKA_STREAM_ID);
  }

  public long getPercentileMetricsComputeWindowSeconds() {
    return config.getLong(PERF_JOB_PERCENTILE_METRICS_COMPUTE_WINDOW_SECONDS,
        DEFAULT_PERF_JOB_PERCENTILE_METRICS_COMPUTE_WINDOW_SECONDS);
  }

  public boolean isBootstrapEnabled() {
    return config.getBoolean(PERF_JOB_BOOTSTRAP_ENABLED, DEFAULT_PERF_JOB_BOOTSTRAP_ENABLED);
  }
}
