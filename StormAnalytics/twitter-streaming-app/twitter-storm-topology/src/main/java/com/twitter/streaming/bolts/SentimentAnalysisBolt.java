package com.twitter.streaming.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.twitter.streaming.analysis.SentimentAnalysis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Bolt implementation to assign sentiment score to text of tweet.
 */
public class SentimentAnalysisBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysisBolt.class);
    private SentimentAnalysis sentimentAnalysis;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            sentimentAnalysis = SentimentAnalysis.getInstance();
        } catch (IOException e) {
            LOG.error("Problem parsing SentimentAnalysis file: " + e.getMessage());
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        double count = 0;
        String text = tuple.getStringByField("tweet_text");

        try {
            //using non-word delimiter.
            String delimiters = "\\W";
            String[] tokens = text.split(delimiters);
            double feeling = 0;
            for (int i = 0; i < tokens.length; ++i) {
                if (!tokens[i].isEmpty()) {
                    // Search as adjective
                    feeling = sentimentAnalysis.extract(tokens[i], "a");
                    count += feeling;
                }
            }

            LOG.info("text: " + text + " count: " + count);
        } catch (Exception e) {
            LOG.error("Problem found when classifying the text: " + e.getMessage());
        }

        collector.emit(new Values(
                tuple.getLongByField("tweet_id"),
                text,
                count,
                tuple.getValueByField("tweet_hashtags"),
                tuple.getStringByField("tweet_created_at")));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet_id", "tweet_text", "tweet_sentiment",
                "tweet_hashtags", "tweet_created_at"));
    }
}
