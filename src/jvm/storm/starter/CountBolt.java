import java.util.Map;
import java.util.HashMap;
import backtype.storm.tuple.Tuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import com.datastax.driver.core.*;

public class CountBolt implements IRichBolt{
	Map<String, Integer> countersQty;
	Map<String, Float> countersPrice;
	private OutputCollector collector;


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			    OutputCollector collector) {

	this.countersQty = new HashMap<String, Integer>();
	this.countersPrice = new HashMap<String, Float>();
	this.collector = collector;
	
	}

	@Override
	public void execute(Tuple input) {
	System.out.println("------>>>> The tuple input is ---->>>> " + input);
	String clordid = input.getString(0);
	String BodyLength = input.getString(1);
	String Msgtype = input.getString(2);
	String SenderCompID = input.getString(3);
	String  Msgseqnum = input.getString(4);
	String Sendingtime = input.getString(5);
	String Price = input.getString(6);
	String OrderQty = input.getString(7);
	String Symbol = input.getString(8);
	String CheckSum = input.getString(9);
       

	//for repeating ID's we accumulate the order qty and price
 	float totPrice = Float.valueOf(Price);
	int totOrderQty = Integer.parseInt(OrderQty);  
	 
	if(!countersQty.containsKey(clordid)){
		countersQty.put(clordid, totOrderQty);
	}else{
		Integer q = countersQty.get(clordid) + totOrderQty;
		countersQty.put(clordid, q);
	}

	if(!countersPrice.containsKey(clordid)){
		countersPrice.put(clordid, totPrice);
	}else{
		Float p = countersPrice.get(clordid) + totPrice;
		countersPrice.put(clordid, p);
	}

	String netPrice = String.valueOf(countersPrice.get(clordid));
	String netQty = String.valueOf(countersQty.get(clordid));

	Cluster clust;
        Session session;
        clust = Cluster.builder().addContactPoint("f3").build();
        session = clust.connect("fixstats");
        
	Statement statement = QueryBuilder.insertInto("fixstats", "order_state")
                .value("clordid", clordid)
                .value("BodyLength", BodyLength)
                .value("Msgtype", Msgtype)
		.value("SenderCompID", SenderCompID)
		.value("Msgseqnum", Msgseqnum)
		.value("Sendingtime", Sendingtime)
		.value("Price", netPrice)
		.value("OrderQty", netQty)
		.value("Symbol", Symbol)
		.value("CheckSum", CheckSum);

	session.execute(statement);
	
	try {
   		 Thread.sleep(1000);                 //1000 milliseconds is one second.
	    } catch(InterruptedException ex) {
    			Thread.currentThread().interrupt();
	   }

	session.close();
        clust.close();	


	collector.ack(input);

}


	@Override
	public void cleanup() {
	//nothin to do!
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("str"));

}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}

