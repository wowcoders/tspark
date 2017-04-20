package org.wowcoders.tspark.models;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum Aggregators {
	CNT("cnt"),
	SUM("sum"),
	AVG("avg"),
	MIN("min"),
	MAX("max");

	private String value;

	Aggregators(String value) {
		this.value = value;
	}

	private static final Map<String, Aggregators> map;
	static {
		map = Arrays.stream(values())
				.collect(Collectors.toMap(e -> e.value, e -> e));
	}

	public String getAggregator() {
		return value;
	}

	public static String[] getAggregators() {
		return Arrays.stream(values()).map(s->s.value).toArray(String[]::new);
	}

	public static Aggregators fromString(String str) {
		return map.get(str);
	}
}