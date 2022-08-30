import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
//import edu.stanford.nlp.sentiment.SentimentAnalysisUtil;

public class MyPositiveBolt extends BaseBasicBolt {
    //@Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String text = ((String) input.getValueByField("related_sentence")).toLowerCase();
            //Analyze the emotion of the sentence
            String emotion = SentimentAnalyzer.findSentiment(text);
            if (emotion.equalsIgnoreCase("Positive")) {
                //If the analysis result is Negative, send the score 1 to bolt
                System.out.println("a " + emotion + " sentiment sentence :" + text);
                collector.emit(new Values(1));
            }
        }catch (Exception e){

        }


    }

    //@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("score"));
    }
}
