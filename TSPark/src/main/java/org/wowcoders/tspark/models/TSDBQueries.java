package org.wowcoders.tspark.models;

import java.io.Serializable;
import java.util.HashMap;

public class TSDBQueries implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5927404387764376086L;
	String metric;
	String aggregator;
	
	HashMap<String, String> tags;
	public String getMetric() {
		return metric;
	}
	public void setMetric(String metric) {
		this.metric = metric;
	}
	public String getAggregator() {
		return aggregator;
	}
	public void setAggregator(String aggregator) {
		this.aggregator = aggregator;
	}
	public HashMap<String, String> getTags() {
		return tags;
	}
	public void setTags(HashMap<String, String> tags) {
		this.tags = tags;
	}
}