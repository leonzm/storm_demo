package com.company.chapter2;

import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

public class OutbreakTrendFactory implements StateFactory {

	private static final long serialVersionUID = 960312319278149956L;

	@Override
	@SuppressWarnings("rawtypes")
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		return new OutbreakTrendState(new OutbreakTrendBackingMap());
	}

}
