package org.wowcoders.tspark.qs;

import java.util.HashMap;

import org.wowcoders.tspark.models.Aggregators;
import org.wowcoders.tspark.models.TSKey;
import org.wowcoders.tspark.models.Topo;
import org.wowcoders.tspark.utils.Pair;

import com.facebook.beringei.thriftclient.Key;

public class TSParkQueryInput {
		Key key;
		Topo topoActual;
		Aggregators agg;
		TSKey topoQuery;
		Topo groupBy = null;

		TSParkQueryInput(Key key, Topo topoActual, Aggregators agg, TSKey topoQuery) {
			this.key = key;
			this.topoActual = topoActual;
			this.agg = agg;
			this.topoQuery = topoQuery;
			try {
				this.groupBy = ((Topo)topoQuery).clone();
				HashMap <String, Pair<String, String>> map = new HashMap <String, Pair<String, String>>();
				this.groupBy.getTags().stream().filter(p->p.second.equals("*")).forEach(p->{
					map.put(p.first, p);
				});
				if (map.size() > 0) {
					topoActual.getTags().stream().forEach(p->{
						if (map.containsKey(p.first)) {
							Pair<String, String> pair = map.get(p.first);
							pair.second = p.second;
						}
					});
					this.groupBy.forceHash();

					this.topoQuery = new TSKey(topoQuery.getAggregator(),
							topoQuery.getMetric(), this.groupBy);
					this.topoQuery.setNamespace(topoQuery.getNamespace());
				}
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}

		public Key getKey() {
			return key;
		}
		public void setKey(Key key) {
			this.key = key;
		}
		public Topo getTopoActual() {
			return topoActual;
		}
		public void setTopoActual(Topo topoActual) {
			this.topoActual = topoActual;
		}
		public Aggregators getAgg() {
			return agg;
		}
		public void setAgg(Aggregators agg) {
			this.agg = agg;
		}

		public TSKey getTopoQuery() {
			return topoQuery;
		}
		public Topo getGroupBy() {
			return groupBy;
		}
	}