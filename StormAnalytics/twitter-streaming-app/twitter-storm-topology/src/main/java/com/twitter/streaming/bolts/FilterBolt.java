package com.twitter.streaming.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

/**
 * Bolt implementation to filter out all non english tweet text.
 */
public class FilterBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FilterBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            JSONObject object = (JSONObject)JSONValue.parseWithException(tuple.getString(0));

            if (object.containsKey("lang") && "en".equals(object.get("lang"))) {
                long id = (long)object.get("id");
                String text = (String)object.get("text");
                String createdAt = (String)object.get("created_at");
                JSONObject entities= (JSONObject)object.get("entities");
                JSONArray hashtags =(JSONArray)entities.get("hashtags");
                HashSet<String> hashtagList = new HashSet<>();
                for(Object hashtag : hashtags)
                {
                    hashtagList.add(((String)((JSONObject)hashtag).get("text")).toLowerCase());
                }

                collector.emit(new Values(id, text, hashtagList, createdAt));
            }
            else {
                LOG.debug("Ignoring non-english tweets");
            }

        } catch (ParseException e) {
            LOG.error("Error parsing tweet: " + e.getMessage());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet_id", "tweet_text", "tweet_hashtags",
                "tweet_created_at"));
    }
}
