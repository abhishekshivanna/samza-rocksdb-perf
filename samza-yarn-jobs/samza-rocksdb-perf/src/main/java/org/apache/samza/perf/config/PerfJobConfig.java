package org.apache.samza.perf.config;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.samza.config.MapConfig;


public class PerfJobConfig extends MapConfig {
  private MapConfig config;

  public PerfJobConfig(MapConfig config) {
    this.config = config;
  }

  public List<String> getKafkaBootstrapServers() {
    final String KAFKA_BOOSTRAP_SERVERS_KEY = "custom.kafka.bootstrap.servers";
    return config.getList(KAFKA_BOOSTRAP_SERVERS_KEY, ImmutableList.of(""));
  }

  public List<String> getConsumerZkConnect() {
    final String CONSUMER_ZK_CONNECT_KEY = "custom.kafka.consumer.zk.connect";
    return config.getList(CONSUMER_ZK_CONNECT_KEY, ImmutableList.of(""));
  }
}
