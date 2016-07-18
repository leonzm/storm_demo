package com.company.chapter2;

import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class DiseaseFilter extends BaseFilter {

	private static final long serialVersionUID = 4363159099120655090L;

	private static final Logger LOG = Logger.getLogger(DiseaseFilter.class);
	
	@Override
	public boolean isKeep(TridentTuple tuple) {
		DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
		Integer code = Integer.parseInt(diagnosis.getDiagnosisCode());
		if (code.intValue() <= 322) {
			LOG.info("Emitting disease [" + diagnosis.getDiagnosisCode() + "]");
			return true;
		} else {
			LOG.info("Filtering disease [" + diagnosis.getDiagnosisCode() + "]");
			return false;
		}
	}
	
}
