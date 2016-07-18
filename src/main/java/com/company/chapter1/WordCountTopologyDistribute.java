package com.company.chapter1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopologyDistribute {

	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	
	public static void main(String[] args) throws Exception {
		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);
		// SentenceSpout --> SplitSentenceBolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).shuffleGrouping(SENTENCE_SPOUT_ID); // shuffleGrouping订阅要求发射的 tuple 随机均匀的分发给 SplitSentenceBolt实例
		// SplitSentenceBolt --> WordCountBolt
		builder.setBolt(COUNT_BOLT_ID, countBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word")); // fieldsGrouping订阅保证素有的"word"字段值相同的 tuple 会被路由到同一个 WordCountBolt 实例
		// WordCountBolt --> ReportBolt
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID); // globalGrouping订阅要求所有的 tuple 路由到唯一的 ReportBolt 任务中
		
		Config config = new Config();
		config.setNumWorkers(2);
		
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			Utils.sleep(10 * 1000);
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
		}
		
	}
	
}
