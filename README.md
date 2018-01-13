## FIX message pipeline :  
  * Input data is encrypted using FIX protocol.  
  * Using mulitple instances of Kafka console producer we are able to push data to a Kafka topic.  
  * The Kafka Spout, subscribing to this topic, passes the data on to the Storm topology.  
  * The SplitBolt class in the Storm topology parses the data as key and value tuples.  
  * The CountBolt class performs simple aggregation on the tuple data based on value in field1 and writes it to Cassandra.      
