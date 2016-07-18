package com.company.chapter1;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6512172977024125900L;

	private HashMap<String, Long> counts = null;
	
	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.counts = new HashMap<String, Long>();
	}

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = tuple.getLongByField("count");
		this.counts.put(word, count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// this bolt does not emit anything
	}

	@Override
	public void cleanup() { // Storm 在终止一个 bolt 之前会调用这个方法。IBolt.cleanup() 方法是不可靠的，不能保证执行
		System.out.println("--- final counts ---");
		counts.forEach((key, value) -> {
			System.out.println(key + ": " + value);
		});
		System.out.println("--------------------");
	}
	
}
