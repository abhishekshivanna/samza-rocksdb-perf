package org.apache.samza.perf;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;


public class TestApplication implements TaskApplication {

  private static final String INPUT_STREAM_ID = "inputStreamId";
  private static final String KAFKA = "kafka";

  private static final String OUTPUT_STREAM_ID = "outputStreamId";


  @Override
  public void describe(TaskApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor inputSystemDescriptor = new KafkaSystemDescriptor(KAFKA);
    KafkaInputDescriptor inputDescriptor = inputSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, new JsonSerdeV2<>(TestEvent.class));

    KafkaSystemDescriptor outputSystemDescriptor = new KafkaSystemDescriptor(KAFKA);
    KafkaOutputDescriptor outputDescriptor = outputSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, new JsonSerdeV2<>(TestEvent.class));

    appDescriptor.withInputStream(inputDescriptor);
    appDescriptor.withOutputStream(outputDescriptor);
    appDescriptor.withTaskFactory(new TestTaskFactory());
  }
}
