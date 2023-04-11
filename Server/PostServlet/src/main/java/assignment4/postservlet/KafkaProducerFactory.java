package assignment4.postservlet;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerFactory {

  public static Producer<String, String> createKafkaProducer() {
    Properties props = new Properties();
    // all these properties could be changed on demands
    // TODO: Please note we can increase the BATCH_SIZE_CONFIG,LINGER_MS_CONFIG AND BUFFER_MEMORY_CONFIG can increase throughput,
    // TODO: but may also effect latency, trade-off, we can adjust those values during performance test
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.28.39:9092,172.31.31.63:9092, 172.31.29.34:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all"); // TODO: this acks config could be changed on demands
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // batch post to kafka
//    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // compress the size of msg to send the broker, increase throughput

    return new KafkaProducer<>(props);
  }
}
