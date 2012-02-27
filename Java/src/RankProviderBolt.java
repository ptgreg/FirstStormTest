import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class RankProviderBolt extends BaseRichBolt {
	private Integer TOP_RATING = 5;
	
	private OutputCollector _collector;
	private List<GenericCount<Integer>> _providers;
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		_providers = new LinkedList<GenericCount<Integer>>();
	}

	@Override
	public void execute(Tuple input) {
		Integer provider = input.getIntegerByField("provider");
		Integer count    = input.getIntegerByField("count");
		
		GenericCount<Integer> providerCount = new GenericCount<Integer>();
		providerCount.Object = provider;
		providerCount.Count  = count;
		
		_providers.add(providerCount);		
		Collections.sort(_providers, Collections.reverseOrder());
		_providers = _providers.subList(0, TOP_RATING);
		
		_collector.emit(new Values(_providers));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topProviders"));
	} 

}
