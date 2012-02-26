
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PrintNumber extends BaseBasicBolt {

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println("Display: " + tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
