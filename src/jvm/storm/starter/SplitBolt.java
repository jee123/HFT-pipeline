package storm.starter;

import java.util.LinkedList;
import java.util.Map;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.task.TopologyContext;
import java.util.List;


public class SplitBolt implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split("\\s+");
        List<String> valuesId = new LinkedList<>();

        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                valuesId.add(word);
            }
        }

        String keyid = "cust_" + valuesId.get(0);
        collector.emit(new Values(keyid, valuesId.get(1), valuesId.get(2), valuesId.get(3), valuesId.get(4),
                valuesId.get(5), valuesId.get(6), valuesId.get(7), valuesId.get(8), valuesId.get(9)));
        collector.ack(input);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field1", "field2", "field3", "field4",
                "field5", "field6", "field7", "field8", "field9", "field10"));
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
