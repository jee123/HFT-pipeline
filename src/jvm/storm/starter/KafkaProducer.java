import java.util.concurrent.CountDownLatch;
import com.google.common.io.Files;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import java.io.File;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.admin.TopicCommand;
import kafka.utils.Time;


public class KafkaProducer {

    private KafkaServer kafkaServer = null;
    private final String topicName;


    CountDownLatch topologyStartedLatch;
    public CountDownLatch producerFinishedInitialBatchLatch = new CountDownLatch(1);
    

    Producer<String, String> producer;

    private String[] sentences;

    KafkaProducer(String[] sentences, String topicName, CountDownLatch topologyStartedLatch) {
        this.sentences = sentences;
        this.topicName = topicName;
        this.topologyStartedLatch = topologyStartedLatch;
    }

    public Thread startProducer() {
        Thread sender = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        emitBatch();
                        ServerAndThreadCoordinationUtils.
                                countDown(producerFinishedInitialBatchLatch);
                        ServerAndThreadCoordinationUtils.
                                await(topologyStartedLatch);
                       // emitBatch();  // emit second batch after we know topology is up
                    }
                },
                "producerThread"
        );
        sender.start();
        return sender;
    }

    private void emitBatch() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        for (String sentence : sentences) {
            KeyedMessage<String, String> data =
                    new KeyedMessage<String, String>(topicName, sentence);
            producer.send(data);
        }
        producer.close();

    }
   
private Properties createProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("broker.id", brokerId + "");
        properties.put("log.dir", logDir);
        //properties.put("zookeeper.connect", "localhost:2000");         // Uses zookeeper created by LocalCluster
        properties.put("zookeeper.connect", "localhost:2181");         // Uses zookeeper created by LocalCluster
        return properties;
    }

}
