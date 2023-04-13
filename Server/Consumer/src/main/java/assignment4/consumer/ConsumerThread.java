package assignment4.consumer;

import assignment4.config.constant.LoadTestConfig;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThread implements Runnable, ConsumerRebalanceListener {
  private final KafkaConsumer<String, String> consumer;
  private final ExecutorService executor = Executors.newFixedThreadPool(LoadTestConfig.CONSUMER_PROCESS_THREAD);
  private final Map<TopicPartition, ProcessTask> activeTasks = new HashMap<>();
  private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private long lastCommitTime = System.currentTimeMillis();
  private final Logger log = LoggerFactory.getLogger(ConsumerThread.class);


  public ConsumerThread(String topic) {

  }


  @Override
  public void run() {

  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> collection) {

  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> collection) {

  }
}
