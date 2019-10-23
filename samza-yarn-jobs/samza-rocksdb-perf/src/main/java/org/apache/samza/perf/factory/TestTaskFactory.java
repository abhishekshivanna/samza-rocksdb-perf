package org.apache.samza.perf.factory;

import org.apache.samza.perf.application.PerfTask;
import org.apache.samza.task.StreamTaskFactory;


public class TestTaskFactory implements StreamTaskFactory {

  @Override
  public PerfTask createInstance() {
    return new PerfTask();
  }
}
