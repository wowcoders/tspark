package org.wowcoders.tspark.qs;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wowcoders.beringeiclient.BeringeiClient;
import org.wowcoders.tspark.models.Aggregators;
import org.wowcoders.tspark.models.TSKey;
import org.wowcoders.tspark.models.Topo;
import org.wowcoders.tspark.tags.AtomixDistributedStore;
import org.wowcoders.tspark.utils.Pair;

import com.facebook.beringei.thriftclient.DataPoint;
import com.facebook.beringei.thriftclient.Key;

public class TaggedTS {
	final static Logger logger = LoggerFactory.getLogger(TaggedTS.class);
	BeringeiClient client = null;
	
	TaggedTS(BeringeiClient client) {
		this.client = client;
	}
	
	Collection<Object> getKeys(TSKey ts) {
		//form the return object with required keys for aggregation
		Topo topo = (Topo)ts;
		String key = topo.hashTopoWithNoStar();
		String mHash = ts.metricHash();
		logger.info("dimkeys for "+mHash+ ":");
		logger.debug("finding dimkeys for metric hash: " + mHash + ":");

		Collection<Object> dimKeys = AtomixDistributedStore.map2.get(mHash).join();/*TODO: hangs if not available*/
		String dimsHash = (String)dimKeys.iterator().next();/*TODO: multiple topo for a metric*/

		logger.debug("metric hash: "+mHash+ ", dimension hash: "+ dimsHash);

		StringBuilder sb = new StringBuilder();
		sb.append(dimsHash);
		sb.append("_");
		sb.append(key);
		sb.append("_");
		sb.append(mHash);
		String lkey = sb.toString();

		{
			long _start = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
			logger.info("key to find the row keys based on filters/tags given ->"+lkey);
			Collection<Object> rowkeys = AtomixDistributedStore.map3.get(lkey).join();
			rowkeys.iterator().forEachRemaining(o -> {
				logger.info("found row key: " + o);
			});
			logger.info("number of row keys found: "+rowkeys.size());

			long _end = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
			logger.info("time taken to retch row keys:" + (_end - _start) + "micros");

			return rowkeys;
		}
	}

	public CompletableFuture <List<DataPoint>> berigeiQueryWrapper(long start, long end, List <Key> keys) {
		CompletableFuture<List<DataPoint>> cfs = new CompletableFuture<List<DataPoint>>();
		List<DataPoint> dps = new ArrayList<DataPoint>();
		long _start = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
		client.getDataPoints(start, end, keys).thenAccept(result -> {
			if (result != null) {
				result.entrySet().stream().forEach(_pair -> {
					Entry<Key,  List<DataPoint>> pair = _pair;
					List<DataPoint> _dps = pair.getValue();

					dps.addAll(_dps);
				});
				logger.info("query complete");
			} else {
				logger.error("query failed");
			}
			long _end = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
			logger.info("Query Overall TimeTaken:" + (_end - _start) + "(micros)");
			cfs.complete(dps);
		}).exceptionally((tw)-> {
			return null;
		});

		return cfs;
	}

	public CompletableFuture <List<TSParkQSResponse>> rollUpAggregation(List<DataPoint> result, Map<String, TSParkQueryInput> inputMap) {
		// ExecutorService pool = Executors.newFixedThreadPool(2);
		CompletableFuture<List<TSParkQSResponse>> cfs = new CompletableFuture<List<TSParkQSResponse>>();

		long _start = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());

		if (result != null) {
			List<TSParkQSResponse> resp = new ArrayList<TSParkQSResponse>();
			Map<String, List<DataPoint>> grpByTopo = result.stream().collect(
					Collectors.groupingBy(dp-> {
						String _key = dp.key.key;
						TSParkQueryInput q = inputMap.get(_key);

						String filter = q.getGroupBy()._hash();
						return filter;
					})
					);
			grpByTopo.entrySet().stream().parallel().forEach(e ->{
				List<DataPoint> _dps = e.getValue();
				if (_dps.size() > 0) {
					String _key = _dps.get(0).getKey().getKey();
					TSParkQueryInput q = inputMap.get(_key);
					Map<Object, DoubleSummaryStatistics> utToVal = null;

					utToVal = _dps.stream().parallel().collect(Collectors.groupingBy(dp-> {
						return dp.value.getUnixTime();
					},  Collectors.summarizingDouble(dp->dp.value.getValue())));

					List<Pair<Long, Double>> dpsByKey = null;

					switch(q.getAgg()) {
					case SUM:
						dpsByKey = utToVal.entrySet().stream()
						.parallel()
						.map(_e -> new Pair<Long, Double>((Long)_e.getKey(), _e.getValue().getSum()))
						.sorted((u1, u2) -> {
							if (u1.second > u2.second) {
								return 1;
							} else if (u1.second < u2.second) {
								return -1;
							} else {
								return 0;
							}
						})
						.collect(Collectors.toList());
						break;
					case CNT:
						dpsByKey = utToVal.entrySet().stream()
						.parallel()
						.map(_e -> new Pair<Long, Double>((Long)_e.getKey(), (double)_e.getValue().getCount()))
						.sorted((u1, u2) -> {
							if (u1.second > u2.second) {
								return 1;
							} else if (u1.second < u2.second) {
								return -1;
							} else {
								return 0;
							}
						})
						.collect(Collectors.toList());
					case AVG:
						dpsByKey = utToVal.entrySet().stream()
						.parallel()
						.map(_e -> new Pair<Long, Double>((Long)_e.getKey(), (double)_e.getValue().getAverage()))
						.sorted((u1, u2) -> {
							if (u1.second > u2.second) {
								return 1;
							} else if (u1.second < u2.second) {
								return -1;
							} else {
								return 0;
							}
						})
						.collect(Collectors.toList());
					case MAX:
						dpsByKey = utToVal.entrySet().stream()
						.parallel()
						.map(_e -> new Pair<Long, Double>((Long)_e.getKey(), _e.getValue().getMin()))
						.sorted((u1, u2) -> {
							if (u1.second > u2.second) {
								return 1;
							} else if (u1.second < u2.second) {
								return -1;
							} else {
								return 0;
							}
						})
						.collect(Collectors.toList());
					case MIN:
						dpsByKey = utToVal.entrySet().stream()
						.parallel()
						.map(_e -> new Pair<Long, Double>((Long)_e.getKey(), _e.getValue().getMax()))
						.sorted((u1, u2) -> {
							if (u1.second > u2.second) {
								return 1;
							} else if (u1.second < u2.second) {
								return -1;
							} else {
								return 0;
							}
						})
						.collect(Collectors.toList());
						break;
					}
					TSParkQSResponse _resp = new TSParkQSResponse(_key,  q.getTopoQuery(), dpsByKey);
					resp.add(_resp);
				}
			});
			logger.info("rollup complete");
			cfs.complete(resp);
		} else {
			logger.error("rollup failed");
			cfs.complete(null);
		}
		long _end = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
		logger.info("rollup Overall TimeTaken:" + (_end - _start) + "(micros)");

		return cfs;
	}

	//executes one m parameter
	CompletableFuture<List<TSParkQSResponse>> getData(long start,
			long end,
			TSKey ts) {
		logger.info("getData working on query " + ts.topoString());
		CompletableFuture<List<TSParkQSResponse>> retDPSCF = new CompletableFuture<List<TSParkQSResponse>>();
		Map<String, TSParkQueryInput> beringeiQIMap = new ConcurrentHashMap<String, TSParkQueryInput>();

		// ExecutorService pool = Executors.newFixedThreadPool(2);
		// TODO need further optimization

		List<Object[]> newList = new ArrayList<Object[]>();

		//get the row keys
		Collection<Object> o = getKeys(ts);

		if (o.size() > 0) {
			o.stream().forEach(key -> {
				Object [] obj = new Object[] {key, ts};
				newList.add(obj);
			});
		}

		Map<Long, List<Object[]>> groupTSKey = new ConcurrentHashMap<Long, List<Object[]>>();
		newList.stream().forEach(obj-> {
			String fullTopoKey = (String)obj[0];
			
			long hashCode = fullTopoKey.hashCode();
			long shardId =  BigInteger.valueOf(hashCode).mod(BigInteger.valueOf(client.getShardCount())).intValue();
			List<Object[]> byShardId = groupTSKey.get(shardId);
			if (byShardId == null) {
				byShardId = new ArrayList<Object[]>();
				groupTSKey.put(shardId, byShardId);
			}
			byShardId.add(obj);
			
			/*a TOPO will get multiple shardIds if we add more hosts/shards into cache cluster */
			Collection<Object> shardIds = AtomixDistributedStore.topoKeyToShardIds.get(fullTopoKey).join();
			shardIds.stream().forEach(_shardId->{
				if (!_shardId.equals(shardId)) {
					List<Object[]> _byShardId = groupTSKey.get(_shardId);
					if (_byShardId == null) {
						_byShardId = new ArrayList<Object[]>();
						groupTSKey.put(shardId, _byShardId);
					}
					_byShardId.add(obj);
				}
			});
		});

		List<DataPoint> dps = new ArrayList<DataPoint>();
		List<CompletableFuture<List<DataPoint>>> cfs = new ArrayList<CompletableFuture<List<DataPoint>>>();
		for(Entry<Long, List<Object[]>> tsArrGrouped: groupTSKey.entrySet()) {
			List <Key> keys = new ArrayList<Key>();
			long shardId = tsArrGrouped.getKey();
			for(Object[] obj: tsArrGrouped.getValue()) {
				Object tsKeyObj = obj[1];
				Key key = new Key();
				
				TSKey tsKey = (TSKey)tsKeyObj;

				String keyByTopo = (String) obj[0];
				Topo tActual = (Topo) AtomixDistributedStore.topoMap.get(keyByTopo).join();
				
				//TODO try to use if preaggregation avg,min,max,sum available tsKey.metricAggHash() or find out the standard way of doing this, right now we use cnt to calculate sum,avg,min,max
				String aggAvailableInDB = "cnt";
				String calcAggMetricHash = TSKey.calcAggMetricHash(aggAvailableInDB, tsKey.getMetric());

				key.key = tsKey.getNamespace() + "_" + calcAggMetricHash + "_" + keyByTopo;
				key.shardId = shardId;
				keys.add(key);

				Aggregators agg = tsKey.getAggregator();
				TSParkQueryInput bwq = new TSParkQueryInput(key, tActual, agg, tsKey);
				beringeiQIMap.put(key.key, bwq);
				logger.debug("inquery:"+((TSKey)tsKeyObj).hash()+", key:"+ key);
			}
			CompletableFuture<List<DataPoint>> cf = berigeiQueryWrapper(start, end, keys);
			cfs.add(cf);
		}
		logger.info("Total batches, runs by shards: " + cfs.size());
		long _start = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
		CompletableFuture.allOf(cfs.toArray(new CompletableFuture[cfs.size()])).thenAccept((result) ->{
			long _end = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
			logger.info("Overall TimeTaken:" + (_end - _start) + "(micros)");

			cfs.stream()
			.map(m->m.join())
			.filter(_dps->_dps != null && _dps.size() > 0)
			.forEach(_dps->dps.addAll(_dps));

			CompletableFuture <List<TSParkQSResponse>> responsesCF = rollUpAggregation(dps, beringeiQIMap);
			responsesCF.thenAccept(responses->retDPSCF.complete(responses));
		});

		return retDPSCF;	
	}
}