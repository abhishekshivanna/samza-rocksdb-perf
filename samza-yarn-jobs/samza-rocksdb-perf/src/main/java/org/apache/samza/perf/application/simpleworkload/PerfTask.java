package org.apache.samza.perf.application.simpleworkload;

import java.time.Duration;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.perf.application.StoreOperation;
import org.apache.samza.perf.application.StoreOperationManager;
import org.apache.samza.perf.config.PerfJobConfig;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PerfTask implements StreamTask, InitableTask, WindowableTask {

  private PerfJobConfig jobConfig;
  private static final Logger LOG = LoggerFactory.getLogger(PerfTask.class);
  private long percentileComputeWindowSeconds;
  private boolean done;
  private long perTaskWindowMs;
  private int numTasks;
  private long nextPctComputeTimeMs;
  private StoreOperationManager storeOperationManager;
  private String taskName;

  @Override
  public void init(Context context) {
    jobConfig = new PerfJobConfig(new MapConfig(context.getJobContext().getConfig()));
    KeyValueStore<String, String> store =
        (KeyValueStore<String, String>) context.getTaskContext().getStore(jobConfig.getStoreName());
    taskName = context.getTaskContext().getTaskModel().getTaskName().getTaskName();
    storeOperationManager = new StoreOperationManager(context, store);
    percentileComputeWindowSeconds = jobConfig.getPercentileMetricsComputeWindowSeconds();
    numTasks = context.getContainerContext().getContainerModel().getTasks().entrySet().size();
    nextPctComputeTimeMs = System.currentTimeMillis();
    done = false;
    perTaskWindowMs = getPerTaskWindowExecutionDurationMs();
    LOG.info("Initialized PerfTask.");
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
      // Determine the operation to do based on the range the random number falls in
      StoreOperation op = storeOperationManager.getNextOperation();
      switch (op) {
        case BOOTSTRAP:
          storeOperationManager.performBootstrap();
          break;
        case GET:
          storeOperationManager.performGet();
          break;
        case INSERT:
          storeOperationManager.performInsert();
          break;
        case UPDATE:
          storeOperationManager.performUpdate();
          break;
        case DELETE:
          storeOperationManager.performDelete();
          break;
        case ALL:
          storeOperationManager.performAll();
          break;
        case RANGE:
          storeOperationManager.performRangeScan();
          break;
        case SNAPSHOT:
          storeOperationManager.performSnapshot();
          break;
        case CLOSE:
          LOG.info("{} store now empty; task will idle", taskName);
          done = true;
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + op);
      }
    }
    if (nextPctComputeTimeMs <= startTime) { // cheaper to compare with startTime instead of System.currentTimeMillis()
      storeOperationManager.computePercentileMetrics();
      nextPctComputeTimeMs += Duration.ofSeconds(percentileComputeWindowSeconds).toMillis();
    }
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
