package assignment4.postservlet;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerFactory {
  private static KafkaProducerFactory instance;
  private static Producer<String, String> producer;


  private KafkaProducerFactory() {
    producer = createKafkaProducer();
  }

  public static synchronized KafkaProducerFactory getInstance() {
    if (instance == null) {
      instance = new KafkaProducerFactory();
    }
    return instance;
  }

  public Producer<String, String> getKafkaProducer() {
    return producer;
  }

  public static Producer<String, String> createKafkaProducer() {
    Properties props = new Properties();
    // all these properties could be changed on demands
    // TODO: Please note we can increase the BATCH_SIZE_CONFIG,LINGER_MS_CONFIG AND BUFFER_MEMORY_CONFIG can increase throughput,
    // TODO: but may also effect latency, trade-off, we can adjust those values during performance test
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.28.39:9092,172.31.31.63:9092");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "1"); // TODO: this acks config could be changed on demands
//    props.put(ProducerConfig.RETRIES_CONFIG, 1);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 5); //TODO: both linger_ms_config and batch_size_config work
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // batch post to kafka
//    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // compress the size of msg to send the broker, increase throughput
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    return new KafkaProducer<>(props);
  }
}
