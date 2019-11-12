package org.ianitrix.kstream.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.Properties;

@Slf4j
public class Main {

  public static void main(String[] args) {
    log.info("Start Stream");

    final Properties config = getStreamConfig();
    final Topology topology = new TopologyStreamBuilder().buildStream();
    log.info(topology.describe().toString());

    final KafkaStreams streams = new KafkaStreams(topology, config);
    streams.start();
    setupShutdownHook(streams);
  }

  public static Properties getStreamConfig() {
    Properties settings = new Properties();
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-examples");
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    settings.put(
            StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            LogAndContinueExceptionHandler.class.getName());
    return settings;
  }

  private static void setupShutdownHook(KafkaStreams streams) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Stop stream");
      streams.close();
    }));
  }
}
