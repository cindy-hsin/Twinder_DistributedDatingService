package assignment4.consumer;

import assignment4.config.constant.KafkaConnectionInfo;
import assignment4.config.constant.LoadTestConfig;

import assignment4.config.constant.MongoConnectionInfo;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;





public abstract class ConsumerThread implements Runnable, ConsumerRebalanceListener {
  private final KafkaConsumer<String, String> consumer;
  private final ExecutorService executor = Executors.newFixedThreadPool(LoadTestConfig.CONSUMER_PROCESS_THREAD);
  private final Map<TopicPartition, ProcessTask> activeTasks = new HashMap<>();
  private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private long lastCommitTime = System.currentTimeMillis();
  private final Logger log = LoggerFactory.getLogger(ConsumerThread.class);

  private MongoClient mongoClient;

  private final String topic;


  public ConsumerThread(String topic, String groupId) {
    // Set up Kafka Consumer, Connect to Kafka Broker
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConnectionInfo.KAFKA_BROKERS_IP);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); //-> We don't specify key for partition.
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    this.consumer = new KafkaConsumer<>(config);
    this.topic = topic;

    // Set up MongoClient

    MongoClientSettings settings = MongoConnectionInfo.buildMongoSettingsForConsumer();

    try {
      this.mongoClient = MongoClients.create(settings);
      System.out.println("Connected to MongoDB!");
    } catch (MongoException me) {
      System.out.println("Failed to create mongoClient: " + me);
    }


  }


  @Override
  public void run() {
    try {
      this.consumer.subscribe(Collections.singleton(this.topic), this);    // subscribe to the topic
      while (!this.stopped.get()) {
        ConsumerRecords<String, String> records = this.consumer.poll(
            LoadTestConfig.CONSUMER_POLL_TIMEOUT);  // poll timeout V.S. max.poll.interval.ms default value: 5 minutes
        this.handleFetchedRecords(records);
        this.checkActiveTasks();
        this.commitOffsets();
      }
    } catch (WakeupException we) {
      if (!this.stopped.get())    // if this.stopped is false, meaning the Consumer is not intentionally stopped (by calling stopConsuming()). ->Some errors have happened and forced Consumer to wake up.
        throw we;
    } finally {
      this.consumer.close();
    }
  }

  /**
   * Check from which partitions we have fetched records into Consumer.
   * Submit records from the same partition to a new ProcessTask to Executor, mark them as activeTasks (i.e. in queue),
   * and Pause polling from those partitions to ensure process order within each partition.
   * @param fetchedRecords
   */
  protected void handleFetchedRecords(ConsumerRecords<String, String> fetchedRecords) {
    if (fetchedRecords.count() > 0) {
      List<TopicPartition> partitionsToPause = new ArrayList<>();
      fetchedRecords.partitions().forEach(partition -> {
        List<ConsumerRecord<String, String>> partitionRecords = fetchedRecords.records(partition);

        ProcessTask task= this.createProcessTask(partitionRecords, this.mongoClient);
        partitionsToPause.add(partition);
        this.executor.submit(task);
        this.activeTasks.put(partition, task);
      });
      consumer.pause(partitionsToPause);   // Pause polling from the same partitions of the records, to Ensure process order within each partition.
    }

  }


  protected abstract ProcessTask createProcessTask(List<ConsumerRecord<String, String>> partitionRecords,MongoClient mongoClient);

  /**
   * Check if each active ProcessTask (of a partition) has finished.
   * If so, consumer resumes fetching data from the corresponding partition, and this partition (and its ProcessTask) is removed from this.activeTasks.
   * Meanwhile, for each active ProcessTask of a partition (whether it's finished or not),
   * update its current offset to the global variable "offsetsToCommit", which will be committed later.
   *
   */
  private void checkActiveTasks() {
    List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
    this.activeTasks.forEach((partition, task) -> {
      if (task.isFinished())
        finishedTasksPartitions.add(partition);
      long offset = task.getCurrentOffset();
      if (offset > 0)
        this.offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
    });
    finishedTasksPartitions.forEach(partition -> this.activeTasks.remove(partition));
    this.consumer.resume(finishedTasksPartitions);
  }

  /**
   * Periodically and manually commit offsets
   */
  private void commitOffsets() {
    try {
      long currentTimeMillis = System.currentTimeMillis();
      if (currentTimeMillis - lastCommitTime > LoadTestConfig.CONSUMER_COMMIT_INTERVAL) {
        if(!this.offsetsToCommit.isEmpty()) {
          this.consumer.commitSync(this.offsetsToCommit);   // Synchronously commit. Ensure commit offsets only after records are processed
          this.offsetsToCommit.clear();
        }
        lastCommitTime = currentTimeMillis;
      }
    } catch (Exception e) {
      System.err.println("Failed to commit offsets! " + e);
      this.log.error("Failed to commit offsets!", e);
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> revokedPartitions) {
    // 1. Stop all tasks handling records from revoked partitions
    Map<TopicPartition, ProcessTask> stoppedTask = new HashMap<>();
    for (TopicPartition partition : revokedPartitions) {
      ProcessTask task = this.activeTasks.remove(partition);    // Remove it from the queue of executor service.
      if (task != null) {
        task.stop();
        stoppedTask.put(partition, task);
      }
    }

    // 2. Wait for stopped tasks to complete processing of current record
    stoppedTask.forEach((partition, task) -> {
      long offset = task.waitForCompletion();
      if (offset > 0)
        this.offsetsToCommit.put(partition, new OffsetAndMetadata(offset));    // update offsets of the stopped-then-completed tasks to the global "offsetsToCommit"
    });

    // 3. collect offsets for revoked partitions (including the stopped-then-completed ones and the ones that are already finished before this onPartitionsAssigned method is triggered.  )
    Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
    revokedPartitions.forEach( partition -> {
      OffsetAndMetadata offset = this.offsetsToCommit.remove(partition);  //Remove from the periodically committed "offsetsToCommit"
      if (offset != null)
        revokedPartitionOffsets.put(partition, offset);                   // And put into another offsets collection that will be immediately committed.
    });

    // 4. commit offsets for revoked partitions (immediately,instead of waiting for CONSUMER_COMMIT_INTERVAL)
    try {
      this.consumer.commitSync(revokedPartitionOffsets);
    } catch (Exception e) {
      System.err.println("Failed to commit offsets for revoked partitions!");
      this.log.warn("Failed to commit offsets for revoked partitions!");
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    this.consumer.resume(partitions);
  }

  public void stopConsuming() {
    this.stopped.set(true);
    this.consumer.wakeup();
  }
}
