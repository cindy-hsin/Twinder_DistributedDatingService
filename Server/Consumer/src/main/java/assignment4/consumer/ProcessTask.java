package assignment4.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class ProcessTask implements Runnable {
  private final List<ConsumerRecord<String, String>> records;

  private volatile boolean stopped = false;

  private volatile boolean started = false;

  private volatile boolean finished = false;

  private final CompletableFuture<Long> completion = new CompletableFuture<>();

  private final ReentrantLock startStopLock = new ReentrantLock();

  private final AtomicLong currentOffset = new AtomicLong();

  private Logger log = LoggerFactory.getLogger(ProcessTask.class);
  public ProcessTask(List<ConsumerRecord<String, String>> records) {
    this.records = records;
  }

  @Override
  public void run() {

  }

  public boolean isFinished() {
    return false;
  }

  public long getCurrentOffset() {
    return 0;
  }

  public void stop() {
  }

  public long waitForCompletion() {
    return -1l;
  }
}
