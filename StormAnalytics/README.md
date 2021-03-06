The solution consists of the following:  
1. **twitter-kafka-producer** : A very basic producer that reads tweets from the Twitter Streaming 
API and stores them in Kafka.  
2. **twitter-storm-topology** : A Storm topology that reads tweets from Kafka and, after applying 
filtering and sanitization, process the messages in parallel for:  
	* Sentiment Analysis: Using a sentiment analysis algorithm to classify the tweet into a 
	positive or negative feeling.    
	* Top Hashtags: Calculates the top 20 hashtags using a sliding window.    
  
Storm topology consists of following:  
* **Kafka Spout**: Spout implementation to read messages from Kafka.  
* **Filtering**: Filtering out all non-english language tweets.  
* **Sanitization**: Text normalization in order to be processed properly by sentiment analysis 
algorithm.  
* **Sentiment analysis** : The algorithm that analyses word by word the text of the tweet, 
giving a value between -1 to 1.  
* **Sentiment analysis to Cassandra** : Stores the tweets and its sentiment value in Cassandra.  
* **Hashtag Splitter** : Splits the different hashtags appearing in a tweet.  
* **Hashtag Counter**: Counts hashtag occurrences  
* **Top Hashtag** : Does a ranking of the top 20 hashtags given a sliding windows (using the Tick 
Tuple feature from Storm).  
* **Top Hashtag to Cassandra** : Stores the top 20 hashtags in Cassandra.   
