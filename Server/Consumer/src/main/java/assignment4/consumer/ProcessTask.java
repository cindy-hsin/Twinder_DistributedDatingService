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

  // volatile: visible to other threads (i.e. ConsumerThread),
  // but doesn't need to be atomic.(because these variables will only be modified
  // in this thread)
  private volatile boolean stopped = false;

  private volatile boolean started = false;

  private volatile boolean finished = false;

  private final CompletableFuture<Long> completionFuture = new CompletableFuture<>();

  private final ReentrantLock startStopLock = new ReentrantLock();

  private final AtomicLong currentOffset = new AtomicLong();

  private Logger log = LoggerFactory.getLogger(ProcessTask.class);
  public ProcessTask(List<ConsumerRecord<String, String>> records) {
    this.records = records;
  }

  @Override
  public void run() {
    // A ProcessTask might be stopped after the thread starts to run
    // but before it starts processing records.
    // In this case, directly exit this method, leaving currentOffset() to be the initial value: 0
    this.startStopLock.lock();
    if (this.stopped){
      return;
    }
    this.started = true;
    this.startStopLock.unlock();

    for (ConsumerRecord<String, String> record : records) {
      if (this.stopped)
        break;      // Ensure that as soon as this task is stopped by ConsumerThread, it will complete the completionFuture with the current offset.
      // TODO: Write to DB (process record here and make sure you catch all exceptions);
      //  If any exception is thrown, complete the completion with exception?? log?

      // The multi-threaded solution here allows us to take as much time as needed to process a record,
      // so we can simply retry processing in a loop until it succeeds.


          this.currentOffset.set(record.offset() + 1);    // Set the currentOffset to the last processed record's offset.
    }
    this.finished = true;
    this.completionFuture.complete(this.currentOffset.get());

  }

  public long getCurrentOffset() {
    return this.currentOffset.get();
  }

  public void stop() {
    this.startStopLock.lock();
    this.stopped = true;
    // Similar to the beginning of run() method,
    // this thread might be stopped before it even starts processing the records (i.e. this.started == false)
    // so in this case we need to properly set the "finished" flag and complete the future with currentOffset(i.e. initial value == 0)
    if (!this.started) {
      this.finished = true;
      this.completionFuture.complete(this.currentOffset.get());
    }
    this.startStopLock.unlock();
  }

  public long waitForCompletion() {
    try {
      return this.completionFuture.get();  // will block until the future is completed.
    } catch (InterruptedException | ExecutionException e) {
      return -1;  // TODO: ExecutionException: might need to log, to see what's wrong on DB side.
    }
  }

  public boolean isFinished() {
    return this.finished;
  }
}
