import java.io.Console;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;

public class HelloFromRabbitSpout extends BaseRichSpout {
	private final static String QUEUE_NAME = "hello";
	
    SpoutOutputCollector _collector;
    Channel _channel;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    
	    try
	    {
		    Connection connection = factory.newConnection();
		    _channel = connection.createChannel();
		    _channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    }
	    catch (Exception e)
	    {
	    	System.out.println(e);
	    }
	}

	@Override
	public void nextTuple() {
		try
	    {
			GetResponse response = _channel.basicGet(QUEUE_NAME, false);
			if (response == null)
				Utils.sleep(50);
			else
			{
			    int number = Integer.parseInt(new String(response.getBody()));    
			    _collector.emit(new Values(number));
			}
	    }
	    catch (Exception e)
	    {
	    	System.out.println(e);
	    }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}
    
    

}
