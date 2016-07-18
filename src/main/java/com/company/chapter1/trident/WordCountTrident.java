package com.company.chapter1.trident;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class WordCountTrident {
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
				new Values("the cow jumped over the moon"), 
				new Values("the man went to the store and bought some candy"),
				new Values("four score and seven years ago"), 
				new Values("how many apples can you eat"));
		spout.setCycle(true);

        TridentTopology topology = new TridentTopology();        
        topology.newStream("spout1", spout)
               .each(new Fields("sentence"), new MySplit(), new Fields("word"))
               .groupBy(new Fields("word"))
               .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
               .parallelismHint(6)
               .newValuesStream()
               .each(new Fields("word", "count"), new MyReport(), new Fields());
        
        Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Word-Count-Trident", config, topology.build());
		
		Utils.sleep(100 * 1000);
		cluster.killTopology("Word-Count-Trident");
		cluster.shutdown();
	}
	
}

class MySplit extends BaseFunction {

	private static final long serialVersionUID = 7320430422807239060L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence = tuple.getString(0);
		for (String word : sentence.split(" ")) {
			collector.emit(new Values(word));
		}
	}

}

class MyReport extends BaseFunction { // 为看到运行的情况加上

	private static final long serialVersionUID = -475908720800273441L;

	private static final Logger LOG = Logger.getLogger(MyReport.class);
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LOG.info("======================> " + tuple.getStringByField("word") + ": " + tuple.getLongByField("count"));
	}
	
}
