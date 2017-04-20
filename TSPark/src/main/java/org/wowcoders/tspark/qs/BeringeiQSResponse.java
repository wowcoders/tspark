package org.wowcoders.tspark.qs;

import java.util.List;

import org.wowcoders.tspark.models.TSKey;
import org.wowcoders.tspark.utils.Pair;

public class BeringeiQSResponse {
		String key;
		TSKey meta;
		List<Pair<Long, Double>> dps;

		public BeringeiQSResponse(String key, TSKey meta, List<Pair<Long, Double>> dps) {
			this.key = key;
			this.meta = meta;
			this.dps = dps;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public TSKey getMeta() {
			return meta;
		}

		public void setMeta(TSKey meta) {
			this.meta = meta;
		}

		public List<Pair<Long, Double>> getDps() {
			return dps;
		}

		public void setDps(List<Pair<Long, Double>> dps) {
			this.dps = dps;
		}
	}