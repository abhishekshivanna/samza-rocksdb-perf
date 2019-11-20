package org.apache.samza.perf.application;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.MapConfig;
import org.apache.samza.perf.config.PerfJobConfig;
import org.apache.samza.perf.serialization.PaddedStringSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;


public class PerfApplication implements TaskApplication {

  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "inputStream";
  private static final String KAFKA_SYSTEM_NAME = "kafka";

  private static final String OUTPUT_STREAM_ID = "outputStream";
  private static PerfJobConfig config;

  @Override
  public void describe(TaskApplicationDescriptor appDescriptor) {
    config = new PerfJobConfig(new MapConfig(appDescriptor.getConfig()));
    int keySizeBytes = config.getStoreKeySizeBytes();

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME);
    kafkaSystemDescriptor.withProducerBootstrapServers(config.getKafkaBootstrapServers());
    kafkaSystemDescriptor.withConsumerZkConnect(config.getConsumerZkConnect());
    kafkaSystemDescriptor.withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor inputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, new NoOpSerde<>());
    KafkaOutputDescriptor outputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, new NoOpSerde<>());

    RocksDbTableDescriptor<String, String> rocksDbTableDescriptor =
        new RocksDbTableDescriptor<>(config.getStoreName(), KVSerde.of(new PaddedStringSerde(keySizeBytes), new StringSerde()));

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor)
        .withInputStream(inputDescriptor)
        .withOutputStream(outputDescriptor)
        .withTable(rocksDbTableDescriptor)
        .withTaskFactory((StreamTaskFactory) PerfTask::new);
  }
}
