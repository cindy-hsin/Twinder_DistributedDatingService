package assignment4.config.constant;

import com.mongodb.ReadConcernLevel;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class LoadTestConfig {
  //  public final static int CONSUMER_THREAD_NUM = 100;   // num of connections from Consumer to DB
//  // The actual DB connection from Consumer = min(CONSUMER_THREAD_NUM, CONSUMER_DB_MAX_CONNECTION)
  public final static int CONSUMER_DB_MAX_CONNECTION = 200;
  public static final int MATCHES_SERVLET_DB_MAX_CONNECTION = 80;
  public static final int STATS_SERVLET_DB_MAX_CONNECTION = 80;

  /**Consumer*/
  public static final int CONSUMER_BATCH_UPDATE_SIZE = 60;   // Consumer.
  public static final int CONSUMER_PROCESS_THREAD = 2;
  public static final Duration CONSUMER_POLL_TIMEOUT = Duration.of(100, ChronoUnit.MILLIS);
  public static final int CONSUMER_COMMIT_INTERVAL = 5000; // Unit: ms

  // Consumer DB BatchUpdate WriteConcern
  public static final int CONSUMER_DB_WRITE_CONCERN = 1; // W=R=1 or
  public static final int CONSUMER_DB_WRITE_TIMEOUT = 100;  // Unit: ms

  public static final ReadConcernLevel CONSUMER_DB_READ_CONCERN_LEVEL = ReadConcernLevel.valueOf(
      "majority");
  // local, available, linearizable
  // Read Concern level: https://www.mongodb.com/docs/manual/reference/read-concern/

  /**Producer*/
  public static final int PRODUCER_BATCH_SIZE = 16384;    // unnit: byte, default 16384

  public static final int PRODUCER_LINGER_MS = 5; //unit:ms, default: 0
  public static final int PRODUCER_MAX_IN_FLIGHT = 5; // default:5


  /**Redis Cache*/
  public static final int REDIS_KEY_EXPIRATION_SECONDS = 60; // unit: second, freshness

  // CONSUMER_THREAD_NUM * CONSUMER_BATCH_UPDATE_SIZE must be divisble by 500K,
  // so that all msgs can be batch updated and ACK to RMQ??
}
