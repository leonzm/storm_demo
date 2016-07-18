package com.company.chapter2;

import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.NonTransactionalMap;

public class OutbreakTrendState extends NonTransactionalMap<Long> {

	protected OutbreakTrendState(IBackingMap<Long> backing) {
		super(backing);
	}

}
