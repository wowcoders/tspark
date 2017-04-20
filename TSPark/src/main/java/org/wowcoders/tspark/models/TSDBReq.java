package org.wowcoders.tspark.models;

import java.io.Serializable;
import java.util.List;

public class TSDBReq implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6383936801886925610L;
	long start = 0;
	long end = 0;
	List <TSDBQueries> queries;
	
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public List<TSDBQueries> getQuery() {
		return queries;
	}
	public void setQuery(List<TSDBQueries> queries) {
		this.queries = queries;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
}