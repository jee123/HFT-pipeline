package storm.starter;
import java.util.Map;

import backtype.storm.tuple.Tuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.task.TopologyContext;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.core.*;

public class CountBolt implements IRichBolt {
    private ConcurrentHashMap<String, Integer> countersQty = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Float> countersPrice = new ConcurrentHashMap<>();
    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String field1 = input.getString(0);
        String field2 = input.getString(1);
        String field3 = input.getString(2);
        String field4 = input.getString(3);
        String field5 = input.getString(4);
        String field6 = input.getString(5);
        String field7 = input.getString(6);
        String field8= input.getString(7);
        String field9 = input.getString(8);
        String field10 = input.getString(9);


        //for repeating value of field1 we accumulate the order qty and price
        Float totPrice = Float.valueOf(field7);
        Integer totOrderQty = Integer.parseInt(field8);

        if (!countersQty.containsKey(field1)) {
            countersQty.put(field1, totOrderQty);
        } else {
            countersQty.put(field1, (countersQty.get(field1) + totOrderQty));
        }

        if (!countersPrice.containsKey(field1)) {
            countersPrice.put(field1, totPrice);
        } else {
            countersPrice.put(field1, (countersPrice.get(field1) + totPrice));
        }

        String netPrice = String.valueOf(countersPrice.get(field1));
        String netQty = String.valueOf(countersQty.get(field1));

        Cluster cluster;
        Session session;
        cluster = Cluster.builder().addContactPoint("q3").build(); /*q3 is hostname of machine.*/
        session = cluster.connect("FIXdata");

        Statement statement = QueryBuilder.insertInto("FIXdata", "order_state")
                .value("field1", field1)
                .value("field2", field2)
                .value("field3", field3)
                .value("field4", field4)
                .value("field5", field5)
                .value("field6", field6)
                .value("field7", netPrice)
                .value("field8", netQty)
                .value("field9", field9)
                .value("field10", field10);

        session.execute(statement);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        session.close();
        cluster.close();

        collector.ack(input);

    }


    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}

