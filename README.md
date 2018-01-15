## StormKafka pipeline :  
  * Input data is encrypted using a protocol like FIX(Financial Information eXchange).  
  * Using mulitple instances of Kafka console producer we are able to push data to a Kafka topic at Network capacity(10GbE).    
  * The Kafka Spout, subscribing to this topic, passes the data on to the Storm topology.    
  * The SplitBolt class in the Storm topology parses the data as key and value tuples.    
  * The CountBolt class performs simple aggregation on the tuple data based on value in field1 and writes it to Cassandra.  
  * We may then query using Cassandra query language(CQL).  
