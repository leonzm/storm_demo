package com.company.chapter2;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class HourAssignment extends BaseFunction {

	private static final long serialVersionUID = -4043642709252985526L;
	
	private static final Logger LOG = Logger.getLogger(HourAssignment.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.get(0);
		String city = (String) tuple.getValue(1);
		
		long timestamp = diagnosis.getTime();
		long hourSinceEpoch = timestamp / 1000 / 60 / 60;
		
		LOG.info("key = [" + city + ":" + hourSinceEpoch + "]");
		String key = city + ":" + diagnosis.getDiagnosisCode() + ":" + hourSinceEpoch;
		
		List<Object> values = new LinkedList<Object>();
		values.add(hourSinceEpoch);
		values.add(key);
		collector.emit(values);
	}

}
