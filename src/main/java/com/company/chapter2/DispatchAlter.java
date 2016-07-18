package com.company.chapter2;

import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class DispatchAlter extends BaseFunction {

	private static final long serialVersionUID = 8279779000288085564L;

	private static final Logger LOG = Logger.getLogger(DispatchAlter.class);
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String alter = (String) tuple.getValue(0);
		LOG.error("Alter received [" + alter + "]");
		LOG.error("Dispatch the national guard!");
		System.exit(0);
	}

}
