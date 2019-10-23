package org.apache.samza.perf;

import org.apache.samza.task.TaskFactory;


public class TestTaskFactory implements TaskFactory {

  @Override
  public TestTask createInstance() {
    return new TestTask();
  }
}
