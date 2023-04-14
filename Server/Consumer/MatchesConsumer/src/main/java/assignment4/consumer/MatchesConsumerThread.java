package assignment4.consumer;

import assignment4.config.constant.KafkaConnectionInfo;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import com.mongodb.client.MongoClient;

public class MatchesConsumerThread extends ConsumerThread{

  public MatchesConsumerThread() {
    super(KafkaConnectionInfo.MATCHES_TOPIC, KafkaConnectionInfo.MATCHES_CONSUMER_GROUP_ID);
  }

@Override
  protected ProcessTask createProcessTask(
      List<ConsumerRecord<String, String>> partitionRecords, MongoClient mongoClient) {
    return new MatchesProcessTask(partitionRecords, mongoClient);
}

  public static void main(String[] args) {
    new Thread(new MatchesConsumerThread()).start();
  }
}
