package com.company.chapter2;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.storm.trident.state.map.IBackingMap;

public class OutbreakTrendBackingMap implements IBackingMap<Long> {

	private static final Logger LOG = Logger.getLogger(OutbreakTrendBackingMap.class);
	
	private Map<String, Long> storage = new ConcurrentHashMap<String, Long>();
	
	@Override
	public List<Long> multiGet(List<List<Object>> keys) {
		List<Long> values = new LinkedList<Long>();
		keys.stream().forEach(key -> {
			Long value = storage.get(key.get(0));
			if (value == null) {
				values.add(new Long(0));
			} else {
				values.add(value);
			}
		});
		
		return values;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<Long> vals) {
		for (int i = 0; i < keys.size(); i ++) {
			LOG.info("Persisting [" + keys.get(i).get(0) + "] ==> [" + vals.get(i) + "]");
			storage.put((String)keys.get(i).get(0), vals.get(i));
		}
	}

}
