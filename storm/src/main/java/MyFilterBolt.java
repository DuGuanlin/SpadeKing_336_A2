import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

/**
 * bolt1 filter data
 */
public class MyFilterBolt extends BaseBasicBolt {
    public void execute(Tuple input, BasicOutputCollector collector) {
//        Status tweet = (Status) input.getValueByField("tweet");
//        String text = tweet.getText().toLowerCase();
        String text = ((String) input.getValueByField("tweet")).toLowerCase();
                //.replaceAll("\\p{Punct}", " ")
                //.replaceAll("\\r|\\n", "").toLowerCase();

        //Only the data with these three tags is retained
        if(text.contains("#covid19") || text.contains("#vaccine") || text.contains("#covidvaccine")){
            System.out.println("find a line related to cvoid19");

            //Remove tag value Then send this data to the downstream bolt
            collector.emit(new Values(text
                    .replace("#covid19","")
                    .replace("#vaccine","")
                    .replace("#covidvaccine","")));
        }else{
            System.out.println("a unvalid string :" + text);
        }

    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("related_sentence"));
    }
}
