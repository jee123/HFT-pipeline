package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

import backtype.storm.StormSubmitter;

public class TestTopology {
    final static int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 10000 /* seconds */;
    private static final int SECOND = 1000;
    private static final String TOPIC_NAME = "Demo";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        if (args == null || args.length == 0) { /*running in local cluster mode of storm*/
            ServerAndThreadCoordinationUtils.setMaxTimeToRunTimer(MAX_ALLOWED_TO_RUN_MILLISECS);
            ServerAndThreadCoordinationUtils.waitForServerUp("q3", 2181, 5 * SECOND);
            ServerAndThreadCoordinationUtils.waitForServerUp("q4", 2181, 5 * SECOND);
            ServerAndThreadCoordinationUtils.waitForServerUp("q5", 2181, 5 * SECOND);
            BrokerHosts brokerHosts = new ZkHosts("q3:2181,q4:2181,q5:2181");
            SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
            kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("wordsp", new KafkaSpout(kafkaConfig), 1);
            builder.setBolt("wordb1", new SplitBolt()).shuffleGrouping("wordsp");
            builder.setBolt("wordb2", new CountBolt()).shuffleGrouping("wordb1");
            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka-test", conf, builder.createTopology());

        } else { /*running in remote mode of storm*/

            ServerAndThreadCoordinationUtils.setMaxTimeToRunTimer(MAX_ALLOWED_TO_RUN_MILLISECS);
            ServerAndThreadCoordinationUtils.waitForServerUp("q3", 2181, 5 * SECOND);
            ServerAndThreadCoordinationUtils.waitForServerUp("q4", 2181, 5 * SECOND);
            ServerAndThreadCoordinationUtils.waitForServerUp("q5", 2181, 5 * SECOND);
            BrokerHosts brokerHosts = new ZkHosts("q3:2181,q4:2181,q5:2181");
            SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
            kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("wordsp", new KafkaSpout(kafkaConfig), 9).setNumTasks(9);
            builder.setBolt("wordb1", new SplitBolt(), 16).shuffleGrouping("wordsp").setNumTasks(16);
            builder.setBolt("wordb2", new CountBolt(), 16).shuffleGrouping("wordb1").setNumTasks(16);
            conf.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }

}

