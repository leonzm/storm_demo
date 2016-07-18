package com.company.chapter2;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;

public class DefaultCoordinator implements BatchCoordinator<Long>, Serializable {
	
	private static final long serialVersionUID = 3416766478654117642L;
	
	private static final Logger LOG = Logger.getLogger(DefaultCoordinator.class);

	@Override
	public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
		LOG.info("Initialize Transaction [" + txid + "]");
		return null;
	}

	@Override
	public void success(long txid) {
		LOG.info("Successful Transaction [" + txid + "]");
	}

	@Override
	public boolean isReady(long txid) {
		return true;
	}

	@Override
	public void close() {
		
	}

}
