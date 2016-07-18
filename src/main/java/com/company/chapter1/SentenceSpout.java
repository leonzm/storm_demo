package com.company.chapter1;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 5806914947822059112L;

	private SpoutOutputCollector collector;
	private String[] sentences = {
		"my dog has fleas",
		"i like cold beverages",
		"the dog age my homework",
		"don't have a cow man",
		"i don't think i like fleas"
	};
	private int index = 0;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence")); // 申明发射包含"sentence"字段的数据流
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index])); // Storm 通过调用这个方法向输出的 collector 发射 tuple
		index ++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.sleep(1);
	}

}
