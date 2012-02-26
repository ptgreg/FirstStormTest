import java.util.HashMap;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class CountNumbersBolt extends BaseRichBolt {

	private OutputCollector _collector; 
	private HashMap<Integer, Integer> _nbCount;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		_nbCount = new HashMap<Integer, Integer>();
	}

	@Override
	public void execute(Tuple input) {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int number = input.getIntegerByField("number");
		
		int nb = 0;
		if (_nbCount.containsKey(number))
			nb = _nbCount.get(number);
		nb += 1;
		_nbCount.put(number, nb);
		
		_collector.emit(new Values(number, nb));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number", "count"));
	}

}
