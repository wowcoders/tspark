package org.wowcoders.beringeiclient;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;

import com.facebook.beringei.thriftclient.BeringeiService;
import com.facebook.beringei.thriftclient.DataPoint;
import com.facebook.beringei.thriftclient.GetDataRequest;
import com.facebook.beringei.thriftclient.GetDataResult;
import com.facebook.beringei.thriftclient.Key;
import com.facebook.beringei.thriftclient.PutDataRequest;
import com.facebook.beringei.thriftclient.PutDataResult;
import com.facebook.beringei.thriftclient.TimeValuePair;

public class BeringeiTestRawClient
{
	static String keyT = "af2e37e6a2792452dea2b69024296faa0";
	static long sec = System.currentTimeMillis() / 1000;

	static final int points = 1;

	static BeringeiService.Client client = null;
	public static void main(String[] args)
	{
		try
		{
			TSocket transport = new TSocket("10.180.17.77", 9999);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);

			client = new BeringeiService.Client(protocol);

			performPut();
			performGet();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
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
			PutDataRequest req = new PutDataRequest();
			req.setData(dps);
			PutDataResult res = client.putDataPoints(req);
			System.out.println(res);
			/*CompletableFuture<List <DataPoint>> droppedCF = client.putDataPoints(req);
			droppedCF.thenAccept(dropped -> {
				if (dropped.size() != 0) {
					System.out.println(dropped);
				}
				latch.countDown();
			});*/
		}
	}
	

	private static void performGet() throws Exception
	{
		List <Key> keys = new ArrayList<Key>();
		Key key = new Key();
		key.key = keyT;
		key.shardId = 2;
		keys.add(key);
		GetDataRequest req = new GetDataRequest();
		req.setKeys(keys);
		req.setBegin(sec-60);
		req.setEnd(sec);
		GetDataResult res = client.getData(req);
		System.out.println(res);
	}
}