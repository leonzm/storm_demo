package com.company.chapter2;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

public class DiagnosisEventSpout implements ITridentSpout<Long> {

	private static final long serialVersionUID = -449549251237777167L;
	
	SpoutOutputCollector collector;
	BatchCoordinator<Long> coordinator = new DefaultCoordinator();
	Emitter<Long> emitter = new DiagnosisEventEmitter();

	@Override
	@SuppressWarnings("rawtypes")
	public BatchCoordinator<Long> getCoordinator( String txStateId, Map conf, TopologyContext context) {
		return coordinator;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
		return emitter;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("event");
	}

}
