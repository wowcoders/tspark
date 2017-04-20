package org.wowcoders.tspark;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.wowcoders.beringeiclient.BeringeiClient;
import org.wowcoders.tspark.models.Aggregators;
import org.wowcoders.tspark.models.TSKey;
import org.wowcoders.tspark.models.Topo;
import org.wowcoders.tspark.tags.AtomixDistributedStore;
import org.wowcoders.tspark.tags.MetaDataCollector;

import com.facebook.beringei.thriftclient.DataPoint;


public class TS {
	List<DataPoint> dps = new ArrayList <DataPoint>();
	BeringeiClient client = null;

	/* aggregated to seconds */
	public TS(BeringeiClient client) {
		this.client = client;
	}

	public void pushTS(TSKey ts, long unixTime, double value) {
		long hashCode = ts.topoHashCode();
		
		long shardId =  BigInteger.valueOf(hashCode).mod(BigInteger.valueOf(client.getShardCount())).intValue();

		this.client.pushTS(ts.getNamespace() + "_" + ts.hash(), 
				shardId,
				unixTime, value);
		
		/*TODO ES, Upload, Topo, metricToTopo, TopoToShardId(Update when changing) */ 

		/*if (ts.getTags().get(1).second.equals("loginhost1000")) {
			System.out.println(ts.toString());
			System.out.println(ts.hashCode() + ":" + ts.topoHashCode());
		}*/
		MetaDataCollector.buildIndex(shardId, (Topo)ts, ts.getMetric(), ts.metricHash());
	}

	public void flush() {
		this.client.flush();
	}

	/* could be second level raw data(number of occurance and sum of values) if cnt and sum are different */
	public void addTSSum(String metric, Topo topo, long unixTime, int count,  double sum) {	
		TSKey tsCount = new TSKey(Aggregators.CNT, metric, topo);
		TSKey tsSum = new TSKey(Aggregators.SUM, metric, topo);

		pushTS(tsCount, unixTime, count);
		pushTS(tsSum, unixTime, sum);
	}
	
	/* could be second level raw data(number of occurance) if cnt and sum are same */
	public void addTSCnt(String metric, Topo topo, long unixTime, int count) {	
		TSKey tsCount = new TSKey(Aggregators.CNT, metric, topo);

		pushTS(tsCount, unixTime, count);
	}
	
	void addTSAvg(String metric, Topo topo, long unixTime, double avg) {	
		TSKey tsAvg = new TSKey(Aggregators.AVG, metric, topo);
		pushTS(tsAvg, unixTime, avg);
	}

	void addTSSumAvg(String metric, Topo topo, long unixTime, int count,  double sum, double avg) {	
		addTSSum(metric, topo, unixTime, count, sum);
		addTSAvg(metric, topo, unixTime, avg);
	}
	
	
	public static void main(String args[]) throws Exception {
		MetaDataCollector.init();
		BeringeiClient client = new BeringeiClient();

		TS ts = new TS(client);

		int hosts = 10;

		long ms = System.currentTimeMillis() / 1000;

		for(int host = 0; host < hosts; host++) {
			Topo topo = new Topo();
			topo.add("pool", "login");
			topo.add("colo", "slc");
			topo.add("host", "loginhost" + host);
			int count = (int) (Math.random() *100);
			int sum = (int) (Math.random() * 1000);
			ts.addTSSum("logincount", topo, ms, count, sum);
		}

		ts.flush();
		System.out.println("Data pushed into Queue.");
		

		
		Topo topo = new Topo();
		topo.add("pool", "login");
		topo.add("colo", "lvs");
		//topo.add("host", "loginhost1");
		
		String key = topo._hash();
		//String dimkey = topo._hashDims();
		TSKey tsSum = new TSKey(Aggregators.AVG, "cpu", topo);
		String mHash = tsSum.metricHash();

		Thread.sleep(8000);
		
		Collection<Object> dimKeys = AtomixDistributedStore.map2.get(mHash).join();
		System.out.println(dimKeys.size());
		String dimsHash = (String)dimKeys.iterator().next();
		
		StringBuilder sb = new StringBuilder();
		sb.append(dimsHash);
		sb.append("_");
		sb.append(key);
		sb.append("_");
		sb.append(mHash);
		String lkey = sb.toString();
		//AtomixDistributedStore.map2.remove(mHash).join();
		//AtomixDistributedStore.map3.remove(lkey).join();

		System.out.println("dimkeys->"+mHash+ ":" + AtomixDistributedStore.map2.get(mHash).join());
		Collection<Object> rowkeys = AtomixDistributedStore.map3.get(lkey).join();
		rowkeys.iterator().forEachRemaining(o -> {
			System.out.println(o);
		});
		System.out.println("-----");
		//AtomixDistributedStore.map3.put("2E2FFC3249D3CC5D1116C4392F4BC584_3CB29AC06397941BDFA5CCCDB54D4F97_12F0E3BB2EB5F19C2B38602A160156AB", "B4CA2DE6AB1570CD780D5AFBF5042CDB").join();
		//System.out.println("key->"+lkey+ ":" + AtomixDistributedStore.map3.get(lkey).join());
		/*System.out.println(topo._hash());
		System.out.println(topo.getIndexes());*/
	}
}