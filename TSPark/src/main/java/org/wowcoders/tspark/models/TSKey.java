package org.wowcoders.tspark.models;

import java.io.Serializable;

import org.wowcoders.tspark.utils.Hash;

public class TSKey extends Topo implements Serializable {
	private static final long serialVersionUID = 1977450313720506886L;

	String namespace = "g";
	String metric;
	String aggregator;

	String calculatedHash = null;
	String metricAggHash = null;
	String metricHash = null;

	public TSKey(Topo t) {
		super(t);
		calculatedHash = null;
	}
	
	public TSKey(Aggregators aggregator, String metric, Topo t) {
		super(t);

		calculatedHash = null;

		this.metric = metric;
		this.aggregator = aggregator.getAggregator();

		hash();
	}
	
	public String getNamespace() {
		return namespace;
	}
	
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public Aggregators getAggregator() {
		return Aggregators.fromString(aggregator);
	}

	public String getMetric() {
		return metric;
	}
	
	public void setAggregator(Aggregators aggregator) {
		this.aggregator = aggregator.getAggregator();
		calculatedHash = null;
	}

	public void setMetric(String metric) {
		this.metric = metric;
		calculatedHash = null;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(aggregator);
		sb.append(":");
		sb.append(metric);
		sb.append(":");
		sb.append(super.toString());
		return sb.toString();
	}

	public String hash() {
		if (calculatedHash == null) {
			metricAggHash = calcAggMetricHash(aggregator, metric);
			
			metricHash = Hash.hash(metric);

			StringBuilder sb = new StringBuilder();
			sb.append(metricAggHash);
			sb.append("_");
			sb.append(super.hash());

			calculatedHash = sb.toString();
		}

		return calculatedHash;
	}
	
	public static String calcAggMetricHash(String aggregator, String metric) {
		StringBuilder metricInfo = new StringBuilder();
		metricInfo.append(aggregator);
		metricInfo.append(":");
		metricInfo.append(metric);

		String metricAggHash = Hash.hash(metricInfo.toString());
		return metricAggHash;
	}

	public int hashCode() {
		String hash = hash();
		//System.out.println("*"+hash);
		return hash.hashCode();
	}

	public int topoHashCode() {
		return super.hashCode();
	}

	public String metricAggHash() {
		if (calculatedHash == null) hash();
		return metricAggHash;
	}
	
	public String metricHash() {
		if (calculatedHash == null) hash();
		return metricHash;
	}
}