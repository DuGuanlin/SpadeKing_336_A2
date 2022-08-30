import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * read twitter data
 */
public class MySpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private TwitterStream twitterStream;
    private LinkedBlockingQueue<Status> queue;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        queue = new LinkedBlockingQueue<Status>(1000);
        this.collector = collector;

        StatusListener listener = new StatusListener() {
            //@Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            //@Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            //@Override
            public void onTrackLimitationNotice(int i) {
            }

            //@Override
            public void onScrubGeo(long l, long l1) {
            }

            //@Override
            public void onStallWarning(StallWarning stallWarning) {
            }

            //@Override
            public void onException(Exception e) {
            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        Properties properties = new Properties();
        try {
            //read twitter api config
            properties.load(new FileInputStream("C:\\Users\\SpadeKing\\Desktop\\storm\\src\\main\\resources\\twitter4j.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String key = properties.getProperty("api_key");
        String api_secret_key = properties.getProperty("api_secret_key");
        String token = properties.getProperty("token");
        String token_secret_key = properties.getProperty("token_secret_key");

//        cb.setOAuthConsumerKey(key);
//        cb.setOAuthConsumerSecret(api_secret_key);
//        cb.setOAuthAccessToken(token);
//        cb.setOAuthConsumerSecret(token_secret_key);
        System.setProperty("twitter4j.oauth.consumerKey", key);
        System.setProperty("twitter4j.oauth.consumerSecret", api_secret_key);
        System.setProperty("twitter4j.oauth.accessToken", token);
        System.setProperty("twitter4j.oauth.accessTokenSecret",token_secret_key);


        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();
    }

    public void nextTuple() {
//        Utils.sleep(1000);
//        collector.emit(new Values(" i dont like cvoid19 #covid19"));
//        collector.emit(new Values("i like cvoid19 #covid19"));
//        collector.emit(new Values("i realy love you #covid19"));
//        collector.emit(new Values("i hate you #covid19"));
//        collector.emit(new Values("i think you should go die #covid19"));
//        collector.emit(new Values("i hate you! #covid19"));
//        collector.emit(new Values("i love you! #covid19"));
//        collector.emit(new Values("there is a pencil #covid19"));
//        collector.emit(new Values("fuck your mother #covid19"));
//        collector.emit(new Values("i want to play the football game #covid19"));
//        collector.emit(new Values("i am so sad because of my pig is lost #covid19"));

        Status ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            System.out.println("read one data from twitter");
            collector.emit(new Values(ret.getText()));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    //@Override
    public void close() {
        twitterStream.shutdown();
    }
}
