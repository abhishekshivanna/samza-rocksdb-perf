package org.apache.samza.perf.application;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.MapConfig;
import org.apache.samza.perf.event.TestEvent;
import org.apache.samza.perf.config.PerfJobConfig;
import org.apache.samza.perf.factory.TestTaskFactory;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;


public class PerfApplication implements TaskApplication {

  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "inputStream";
  private static final String KAFKA_SYSTEM_NAME = "kafka";

  private static final String OUTPUT_STREAM_ID = "outputStream";
  private static PerfJobConfig config;

  @Override
  public void describe(TaskApplicationDescriptor appDescriptor) {
    config = new PerfJobConfig(new MapConfig(appDescriptor.getConfig()));

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME);
    kafkaSystemDescriptor.withProducerBootstrapServers(config.getKafkaBootstrapServers());
    kafkaSystemDescriptor.withConsumerZkConnect(config.getConsumerZkConnect());
    kafkaSystemDescriptor.withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor inputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, new JsonSerdeV2<>(TestEvent.class));
    KafkaOutputDescriptor outputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, new JsonSerdeV2<>(TestEvent.class));

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);
    appDescriptor.withInputStream(inputDescriptor);
    appDescriptor.withOutputStream(outputDescriptor);
    appDescriptor.withTaskFactory(new TestTaskFactory());
  }
}
