package org.wowcoders.beringeiclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.wowcoders.beringeiclient.configurations.Configuration;

import com.facebook.beringei.thriftclient.DataPoint;
import com.facebook.beringei.thriftclient.Key;
import com.facebook.beringei.thriftclient.TimeValuePair;

public class BeringeiTestClient {
	static String keyT = "af2e37e6a2792452dea2b69024296faa0";
	static long sec = System.currentTimeMillis() / 1000;

	static final int points = 1;

	private static final CountDownLatch latch = new CountDownLatch(points);

	static BeringeiClient client = null;

	public static void main(String [] args) throws InterruptedException {
		Configuration.loadConfig();
		try {
			client = new BeringeiClient();
			performPut();
			Thread.sleep(5000);
			performGet();
		} catch (Exception x) {
			x.printStackTrace();
		} 
		while(true) {
			Thread.sleep(250);

			if (latch.getCount() == 0) {
				System.out.println("done");
				System.exit(0);
			}
		}
	}

	private static void performPut() throws Exception
	{

		List <DataPoint> dps = new ArrayList<DataPoint>();
		Key key = new Key();
		key.key = keyT;
		key.shardId = 2;

		DataPoint dp = new DataPoint();
		dp.setKey(key);
		TimeValuePair tvp = new TimeValuePair();

		tvp.setUnixTime(sec);

		tvp.setValue(10);
		dp.setValue(tvp);

		dps.add(dp);

		for(int i = 0; i < points; i++) {
			CompletableFuture<List <DataPoint>> droppedCF = client.putDataPoints(dps);
			droppedCF.thenAccept(dropped -> {
				if (dropped.size() != 0) {
					System.out.println(dropped);
				}
				latch.countDown();
			});
		}
	}

	private static void performGet() throws Exception
	{
		List <Key> keys = new ArrayList<Key>();
		Key key = new Key();
		key.key = keyT;
		key.shardId = 2;
		keys.add(key);
		CompletableFuture<Map<Key, List<DataPoint>>> dpsCF = client.getDataPoints(sec-60, sec, keys);
		dpsCF.thenAccept(dps -> {
			System.out.println(dps);
		});
	}
}


