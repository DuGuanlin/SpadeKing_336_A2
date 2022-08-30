import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import java.io.IOException;

public class MyTopology {

    public static void main(String[] args) throws IOException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-reader", new MySpout());//spout read twritter data
        builder.setBolt("segement-filter", new MyFilterBolt()).shuffleGrouping("twitter-reader");//bolt filter data by segement

        builder.setBolt("negative-bolt", new MyNegativeBolt()).shuffleGrouping("segement-filter"); //get data from bolt:'segement-filter' then analy the negative emotion
        builder.setBolt("positive-bolt", new MyPositiveBolt()).shuffleGrouping("segement-filter");//get data from bolt:'segement-filter' then analy the positive  emotion

        //Count the total score of the two
        builder.setBolt("score", new MyScoreBolt())
                .shuffleGrouping("negative-bolt")
                .shuffleGrouping("positive-bolt");


        Config conf = new Config();
        conf.setDebug(false);
        //Run in local mode
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("twitter emotion analyse", conf, builder.createTopology());
    }
}
