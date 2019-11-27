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
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;


public class PerfApplication implements TaskApplication {

  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");
  private static PerfJobConfig config;

  @Override
  public void describe(TaskApplicationDescriptor appDescriptor) {
    config = new PerfJobConfig(new MapConfig(appDescriptor.getConfig()));
    int keySizeBytes = config.getStoreKeySizeBytes();

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(config.getInputSystemName());
    kafkaSystemDescriptor.withProducerBootstrapServers(config.getKafkaBootstrapServers());
    kafkaSystemDescriptor.withConsumerZkConnect(config.getConsumerZkConnect());
    kafkaSystemDescriptor.withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor inputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(config.getInputStreamId(), new NoOpSerde<>());

    RocksDbTableDescriptor<String, String> rocksDbTableDescriptor = new RocksDbTableDescriptor<>(config.getStoreName(),
        KVSerde.of(new PaddedStringSerde(keySizeBytes), new StringSerde()));

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor)
        .withInputStream(inputDescriptor)
        .withTable(rocksDbTableDescriptor)
        .withTaskFactory((StreamTaskFactory) PerfTask::new);
  }
}
