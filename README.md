<<<<<<< HEAD
# HighFrequencyTrading
=======
# HFT-pipeline
>>>>>>> 46cc4ca1f16e9cc682ac2ca41226c9f20d16713a
FIX messages, Apache Kafka, Storm and Cassandra
Input data is genereated using FIX (Financial Information eXchange) Engine implemented in Python.This data is then relayed to Apache Kafka that passes it on to the Storm topology via KafkaSpout.  
The SplitBolt class in the Storm topology parses the data as key and value tuples.The CountBolt class performs aggregation on the tuple data based on the ClordId and writes it to Cassandra.  
