package org.wowcoders.tspark.tags;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wowcoders.tspark.configurations.Configuration;
import org.wowcoders.tspark.models.Topo;

public class MetaDataCollector {
	final static Logger logger = LoggerFactory.getLogger(MetaDataCollector.class);
	
	static Map<String, Topo> all = new ConcurrentHashMap<String, Topo>();
	static Map<String, Topo> mapKeysToTopo = new ConcurrentHashMap<String, Topo>();
	static Map<String, ConcurrentHashMap<String, String>> topoKeysToMetricKeys = new ConcurrentHashMap<String, ConcurrentHashMap<String, String>>();
	static Map<String, Long> topoKeysToShardIds = new ConcurrentHashMap<String, Long>();
	
	public static void buildIndex(long shardId,
			Topo t,
			String metric,
			String mHash) {
		String thash = t._hash();
		
		all.put(thash, t);
		
		if (t.updateRequired()) {
			if (!mapKeysToTopo.containsKey(thash)) {
				mapKeysToTopo.put(thash, t);
			}
		}
		
		if (!topoKeysToMetricKeys.containsKey(thash)) {
			topoKeysToMetricKeys.put(thash,  new ConcurrentHashMap<String, String>());
		}
		
		ConcurrentHashMap<String, String> mmap = topoKeysToMetricKeys.get(thash);
		if (mmap != null) {
			mmap.put(mHash, metric);
		}
		
		topoKeysToShardIds.put(thash, shardId);
	}
	
	public static void init() {
		Configuration cfg = Configuration.getInstnace();
		AtomixDistributedStore.start(cfg.getTagsCacheConfig().getListenAddress(), cfg.getTagsCacheConfig().getCluster()).join();
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				flush();
			}
		}, 5000, 60000);
	}
	
	public static Thread flush() {
		Thread t = new Thread() {
			@Override
			public void run() {
				Configuration cfg = Configuration.getInstnace();
				long cacheTTL = cfg.getTagsCacheConfig().getTtl();
				final Map<String, Topo> mapPrevKeysToTopo = mapKeysToTopo;
				mapKeysToTopo = new ConcurrentHashMap<String, Topo>();
				final Map<String, ConcurrentHashMap<String, String>> prevTopoKeysToMetricKeys = topoKeysToMetricKeys;
				final Map<String, Long> prevTopoKeysToShardIds = topoKeysToShardIds;
				topoKeysToMetricKeys = new ConcurrentHashMap<String, ConcurrentHashMap<String, String>>();
				topoKeysToShardIds = new ConcurrentHashMap<String, Long>();
				
				/* write tagk, tagv for suggest */
				for(Entry<String, Topo> entry : mapPrevKeysToTopo.entrySet()) {
					Topo t = entry.getValue();
					String key = t._hash();
					AtomixDistributedStore.topoMap.put(key, t, Duration.ofDays(8));
					t.getTags().stream().forEach(pair -> {
						AtomixDistributedStore.tagk.put(pair.first, true,  Duration.ofDays(8));
						AtomixDistributedStore.tagv.put(pair.second, true,  Duration.ofDays(8));
					});
					
					/*List<String> []indexes = t.getIndexes();
					for(String index : indexes[0]) {
						//System.out.println("MapsToInsert:"+index+":"+key);
						AtomixDistributedStore.map1.put(index, key, Duration.ofDays(8));
					}
					for(String index : indexes[1]) {
						// System.out.println("MapsToInsert:"+index+":"+key);
						AtomixDistributedStore.map2.put(index, key, Duration.ofDays(8));
					}*/
				}
				
				/* TODO trigram for regex on values */
				
				StringBuilder sb = new StringBuilder();
				for(Entry<String, ConcurrentHashMap<String, String>> entry :prevTopoKeysToMetricKeys.entrySet()) {
					String tkey = entry.getKey();
					
					Topo t = all.get(tkey);
					String hashDims = t._hashDims();
					sb.append(hashDims);
					sb.append("_");
					int length1 = sb.length();
					List<String> []indexes = t.getIndexes();
					for(String index : indexes[0]) {
						sb.append(index);
						sb.append("_");
						
						int length2 = sb.length();
						entry.getValue().entrySet().stream().forEach(info -> {
							String mkey = info.getKey();
							AtomixDistributedStore.map2.put(mkey, hashDims, Duration.ofMillis(cacheTTL));
							sb.append(mkey);
							AtomixDistributedStore.map3.put(sb.toString(), tkey, Duration.ofMillis(cacheTTL));
							logger.debug("inserted topo key ->"+ tkey+", filter key: "+ sb);
							sb.setLength(length2);
							
							AtomixDistributedStore.metrics.put(info.getValue(), true, Duration.ofMillis(cacheTTL));
						});
						sb.setLength(length1);
					}
					
					sb.setLength(0);
				}
				
				for(Entry<String, Long> entry :prevTopoKeysToShardIds.entrySet()) {
					logger.debug("inserted topo key to shardid ->"+ entry.getKey() +", shard keyid: "+ entry.getValue());
					//TODO store hosts served the shardid ?
					//need to decide how we are going linearly scale.  
					//we need to store host only if we are adding new boxes and move the shardid to new boxes.
					//while adding new boxes, if we have the hosts fixed for shardsid and we just assign the new range
					AtomixDistributedStore.topoKeyToShardIds.put(entry.getKey(), entry.getValue(), Duration.ofMillis(cacheTTL));
				}
			}
		};
		t.start();
		return t;
	}
}