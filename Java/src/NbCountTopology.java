import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class NbCountTopology {

	 public static void main(String[] args) throws Exception {
	        
	        TopologyBuilder builder = new TopologyBuilder();
	        
	        // Create Topology
	        builder.setSpout("Get-SimpleStruct", new SimpleStructFromRabbitSpout(), 2);
	        
	        builder.setBolt("Count-Number", new CountNumbersBolt(), 3)
	        	   .fieldsGrouping("Gen-Number", new Fields("number"));
	        
	        builder.setBolt("Print-Number", new PrintNumber(), 1)
	        	   .shuffleGrouping("Count-Number");
	        
	        // Launch local cluster
	        Config conf = new Config();
	        conf.setDebug(true);
	        conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
        
            Thread.sleep(60 * 1000);

            cluster.shutdown();
	 }
}
