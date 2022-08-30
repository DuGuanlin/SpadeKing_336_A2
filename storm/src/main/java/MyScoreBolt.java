import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MyScoreBolt  extends BaseBasicBolt {
    private int positive_socre = 0;
    private int negative_socre = 0;

    public void execute(Tuple input, BasicOutputCollector collector) {
        int score = ((Integer) input.getValueByField("score"));
        //Receive the data of two upstream bolts and count their total scores respectively
        if(score > 0){
            positive_socre += score;
            System.out.println("get a positive sentence,positive score is: "+ positive_socre);
        }else{
            System.out.println("get a negative sentence,negative score is: "+ negative_socre);
            negative_socre += score;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
