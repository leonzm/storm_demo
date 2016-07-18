package com.company.chapter2;

import java.io.Serializable;

public class DiagnosisEvent implements Serializable {

	private static final long serialVersionUID = -2456669783148329383L;

	private double lat;
	private double lng;
	private long time;
	private String diagnosisCode;
	
	public DiagnosisEvent(double lat, double lng, long time, String diagnosisCode) {
		this.lat = lat;
		this.lng = lng;
		this.time = time;
		this.diagnosisCode = diagnosisCode;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLng() {
		return lng;
	}

	public void setLng(double lng) {
		this.lng = lng;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getDiagnosisCode() {
		return diagnosisCode;
	}

	public void setDiagnosisCode(String diagnosisCode) {
		this.diagnosisCode = diagnosisCode;
	}
}
