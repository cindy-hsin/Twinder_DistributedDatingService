package assignment4.config.constant;

public class LoadTestConfig {
//  public final static int POST_SERVLET_CHANNEL_POOL_SIZE = 30; //Tomcat MAX 200
//  public final static int CONSUMER_THREAD_NUM = 100;   // num of connections from Consumer to DB
//  // The actual DB connection from Consumer = min(CONSUMER_THREAD_NUM, CONSUMER_DB_MAX_CONNECTION)
//  public final static int CONSUMER_DB_MAX_CONNECTION = 200;
  public static final int MATCHES_SERVLET_DB_MAX_CONNECTION = 80;
  public static final int STATS_SERVLET_DB_MAX_CONNECTION = 80;



  // Consumer
  public static final int BATCH_UPDATE_SIZE = 60;   // Consumer.
  public static final int CONSUMER_PROCESS_THREAD = 2;




  // CONSUMER_THREAD_NUM * BATCH_UPDATE_SIZE must be divisble by 500K,
  // so that all msgs can be batch updated and ACK to RMQ??
}
