import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import storm.kafka.KafkaConfig;
import java.net.Socket;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.Nimbus.Client;

public class TestTopology {
    final static int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 10000 /* seconds */;
    CountDownLatch topologyStartedLatch = new CountDownLatch(1);
    private static final int SECOND = 1000;
    private static List<String> messagesReceived = new ArrayList<String>();
    private static final String TOPIC_NAME = "Demo";
    volatile static boolean finishedCollecting = false;
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        CountDownLatch topologyStartedLatch = new CountDownLatch(1);
        if (args == null || args.length == 0) {
            ServerAndThreadCoordinationUtils.setMaxTimeToRunTimer(MAX_ALLOWED_TO_RUN_MILLISECS);
	    ServerAndThreadCoordinationUtils.waitForServerUp("f3", 2181, 5 * SECOND);
	    ServerAndThreadCoordinationUtils.waitForServerUp("f4", 2181, 5 * SECOND);
	    ServerAndThreadCoordinationUtils.waitForServerUp("f5", 2181, 5 * SECOND);
	    BrokerHosts brokerHosts = new ZkHosts("f3:2181,f4:2181,f5:2181");
	    SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
	    kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	    builder.setSpout("wordsp", new KafkaSpout(kafkaConfig), 1);
	    builder.setBolt("wordb1", new SplitBolt()).shuffleGrouping("wordsp");
            builder.setBolt("wordb2", new CountBolt()).shuffleGrouping("wordb1");	
	    conf.setMaxTaskParallelism(2);
	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("kafka-test", conf, builder.createTopology());	
        }
        else {
	    ServerAndThreadCoordinationUtils.setMaxTimeToRunTimer(MAX_ALLOWED_TO_RUN_MILLISECS);
            ServerAndThreadCoordinationUtils.waitForServerUp("f3", 2181, 5 * SECOND);
            ServerAndThreadCoordinationUtils.waitForServerUp("f4", 2181, 5 * SECOND);
            ServerAndThreadCoordinationUtils.waitForServerUp("f5", 2181, 5 * SECOND);	    
	    BrokerHosts brokerHosts = new ZkHosts("f3:2181,f4:2181,f5:2181");
            SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
            kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("wordsp", new KafkaSpout(kafkaConfig), 9).setNumTasks(9);
            builder.setBolt("wordb1", new SplitBolt(),16).shuffleGrouping("wordsp").setNumTasks(16);
            builder.setBolt("wordb2", new CountBolt(),16).shuffleGrouping("wordb1").setNumTasks(16);
	    conf.setNumWorkers(2);
	    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
    
}

