import java.util.Map;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.task.TopologyContext;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;



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
	    String[] words = sentence.split(" ");
	    List<String> valsid = new ArrayList<String>(10);
	    for (String word:words){
	        word = word.trim();
	        if (!word.isEmpty()) {	
	            valsid.add(word);
	        }
            }

	String keyid = "cust_" + valsid.get(0);
	collector.emit(new Values(keyid, valsid.get(1), valsid.get(2), valsid.get(3), valsid.get(4), valsid.get(5), valsid.get(6), valsid.get(7), valsid.get(8), valsid.get(9)));
							
	collector.ack(input);
        }


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("clordid", "BodyLength", "Msgtype", "SenderCompID", "Msgseqnum", "Sendingtime", "Price", "OrderQty", "Symbol", "CheckSum"));
		//declarer.declare(new Fields("word"));
	}

	@Override
	public void cleanup() {
	//nothing to do!
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
	return null;
	}
}
