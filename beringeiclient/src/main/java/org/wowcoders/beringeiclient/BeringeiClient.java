package org.wowcoders.beringeiclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wowcoders.beringei.compression.BlockDecoder;
import org.wowcoders.beringeiclient.configurations.Configuration;
import org.wowcoders.beringeiclient.utils.Pair;

import com.facebook.beringei.thriftclient.DataPoint;
import com.facebook.beringei.thriftclient.GetDataRequest;
import com.facebook.beringei.thriftclient.GetDataResult;
import com.facebook.beringei.thriftclient.Key;
import com.facebook.beringei.thriftclient.PutDataRequest;
import com.facebook.beringei.thriftclient.PutDataResult;
import com.facebook.beringei.thriftclient.StatusCode;
import com.facebook.beringei.thriftclient.TimeSeriesBlock;
import com.facebook.beringei.thriftclient.TimeSeriesData;
import com.facebook.beringei.thriftclient.TimeValuePair;
import com.facebook.beringei.thriftclient.BeringeiService.Client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

import cn.danielw.fop.ObjectFactory;
import cn.danielw.fop.ObjectPool;
import cn.danielw.fop.PoolConfig;
import cn.danielw.fop.Poolable;


public class BeringeiClient {
	final static Logger logger = LoggerFactory.getLogger(BeringeiClient.class);

	static int shardCount = 100;

	static int flushAfterCount = 1000;
	static int flushInterval = 10000;

	static int executerReaderThreads = 20; 
	static int executerWriterThreads = 10;

	static ExecutorService executorReader = Executors.newFixedThreadPool(executerReaderThreads);
	static ExecutorService executorWriter = Executors.newFixedThreadPool(executerWriterThreads);

	//Map<Long, List<Client>> clusterByShards = new ConcurrentHashMap<Long, List<Client>>();
	//Map<Long, List<DataPoint>> batchByShards = new ConcurrentHashMap<Long, List<DataPoint>>();
	Map<Long, List<ObjectPool<Client>>> clusterByShards = new ConcurrentHashMap<Long, List<ObjectPool<Client>>>();
	Map<Long, List<DataPoint>> batchByShards = new ConcurrentHashMap<Long, List<DataPoint>>();
	
	private ObjectFactory<Client> createObjectFactory(Pair<String, Integer> addr) {
		ObjectFactory<Client> factory = new ObjectFactory<Client>() {
			@Override public Client create() {
				Configuration cfg = Configuration.getInstnace();

				TSocket socket = new TSocket(addr.first, addr.second);

				socket.setTimeout((int)(cfg.getClientConfig().getConnectTimeout() 
						+ cfg.getClientConfig().getWriteTimeout()
						+ cfg.getClientConfig().getReadTimeout()));
				

				try {
					socket.open();
				} catch (TTransportException e) {
					e.printStackTrace();
				}
				TProtocol protocol = new TBinaryProtocol(new TFramedTransport(socket));
				Client client = new Client(protocol);
				return client;
			}
			@Override public void destroy(Client o) {
				try {
					o.getInputProtocol().getTransport().close();
					o.getOutputProtocol().getTransport().close();
				} catch(Exception e) {

				}
			}
			@Override public boolean validate(Client o) {
				return o.getInputProtocol().getTransport().isOpen() && o.getOutputProtocol().getTransport().isOpen();
			}
		};

		return factory;
	}
	
	@SuppressWarnings("serial")
	public BeringeiClient() throws IOException {
		Configuration cfg = Configuration.getInstnace();

		shardCount = cfg.getClientConfig().getShardCounts();

		flushAfterCount = cfg.getClientConfig().getFlushAfterCount();
		flushInterval = cfg.getClientConfig().getFlushInterval();

		//TODO need review around how we creates pools for async vs threads
		executerReaderThreads = cfg.getClientConfig().getReadThreads(); 
		executerWriterThreads = cfg.getClientConfig().getWriteThreads();

		PoolConfig config = new PoolConfig();
		config.setPartitionSize(5);
		config.setMaxSize(executerReaderThreads + executerWriterThreads);
		config.setMinSize(cfg.getClientConfig().getIdleConnectionsPerShards());
		config.setMaxIdleMilliseconds((int)cfg.getClientConfig().getTimeoutToCloseIdleConnection());

		//TODO 1. Multidatacenter support,
		//2. refresh the map if it is configured using admin ui.
		//3. event endpoints as backup/can all beringei nodes supports read of key with any shardid?

		String [] datacenters = cfg.getClientConfig().getDatacenters();
		String datacenter = datacenters[0];
		HashSet<Pair<String, Integer>> hostSets =  cfg.getClientConfig().getDatacenterHostListMap().get(datacenter);

		int hostCount = hostSets.size();
		int shardsPerHost = shardCount/hostCount;

		Iterator<Pair<String, Integer>> it = hostSets.iterator();
		int startShard = 0;
		// System.out.println(hostCount + ":" + shardCount + ":" +  shardsPerHost);
		while(it.hasNext()) {
			Pair<String, Integer> hostAddr = it.next();
			// System.out.println("hostaddr"+hostAddr.first);
			ObjectPool<Client> cliPool = new ObjectPool<Client>(config, createObjectFactory(hostAddr));
			for(long i = startShard; i < shardCount; i++) {
				clusterByShards.put(i, new ArrayList<ObjectPool<Client>>() {{
					add(cliPool);
				}});
				batchByShards.put(i, new ArrayList<DataPoint>());
				// System.out.println("added shard" +  i);
			}
			startShard += shardsPerHost;
		}

		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				flush();
			}
		}, flushInterval, flushInterval);
	}

	public void flush() {
		for(long shardId = 0; shardId < shardCount; shardId++) {
			List <DataPoint> dpsPrev = batchByShards.get(shardId);
			batchByShards.put(shardId, new ArrayList <DataPoint>());

			if (dpsPrev.size() != 0) {
				CompletableFuture<List <DataPoint>> droppedCF = putDataPoints(dpsPrev);
				droppedCF.thenAccept(dropped -> {
					if (dropped != null) {
						if (dropped.size() != 0) {
							logger.error("Dropped size" + dropped);
						} else {
							logger.debug("successfully sent data points to beringei server.");
						}
					} else {
						logger.error("failed to send data points to beringei server.");
					}
				}).exceptionally(tw -> {
					return null;
				});
			}
		}
	}

	public int getShardCount() {
		return shardCount;
	}

	public void pushTS(String hash, long shardId, long unixTime, double value) {
		Key key = new Key();
		key.key = hash;
		key.shardId = shardId;

		TimeValuePair tvp = new TimeValuePair();
		tvp.setUnixTime(unixTime);
		tvp.setValue(value);

		DataPoint dp = new DataPoint();
		dp.setKey(key);
		dp.setValue(tvp);
		dp.setCategoryId(0);  // TODO how are going to categorize, can it serve the purpose of CNT, SUM ....

		List<DataPoint> dps = batchByShards.get(shardId);

		dps.add(dp);

		if (dps.size() == flushAfterCount) {
			flush();
		}
	}
	
	
	public CompletableFuture<List <DataPoint>> putDataPoints(List <DataPoint> dps) {
		CompletableFuture<List <DataPoint>> completableFuture =  new CompletableFuture<List <DataPoint>>();
		executorWriter.submit(() -> {
			PutDataRequest req = new PutDataRequest();
			req.setData(dps);

			long shardId = dps.get(0).getKey().getShardId();

			List<ObjectPool<Client>> cliList = clusterByShards.get(shardId);
			PutDataResult  res = null;
			int saved = 0;
			for(ObjectPool<Client>  pool: cliList) {
				int ntry = 1;
				while (ntry < 3) {
					ntry++;
					Poolable<Client> obj = null;
					try {
						obj = pool.borrowObject(true);
						Client client = obj.getObject();
						if (!client.getOutputProtocol().getTransport().isOpen()) {
							client.getOutputProtocol().getTransport().open();
						}
						res = client.putDataPoints(req);
						saved++;
						break;
					} catch (TException e) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
						}
						try {
							Client client = obj.getObject();
							client.getInputProtocol().getTransport().close();
							client.getOutputProtocol().getTransport().close();
						} catch(Exception e1) {

						}
					} finally {
						if(null != obj) {
							pool.returnObject(obj);
						}
					}
				}

				if (ntry == 3) {
					logger.error("Put Exception, Shard Id" + dps.get(0).getKey().shardId + ", " + saved);
				}
			}

			if (saved > 0) {
				completableFuture.complete(res.getData());
			} else {
				completableFuture.completeExceptionally(null); //TODO ?
			}
		});
		return completableFuture;
	}


	public CompletableFuture<Map<Key, List <DataPoint>>> getDataPoints(long start, long end, List <Key> keys) {
		CompletableFuture<Map<Key, List <DataPoint>>> completableFuture =  new CompletableFuture<Map<Key, List <DataPoint>>>();
		executorReader.submit(() -> {
			GetDataRequest req = new GetDataRequest();

			req.setBegin(start);
			req.setEnd(end);
			req.setKeys(keys);

			long shardId = keys.get(0).getShardId();

			List<ObjectPool<Client>> cliList = clusterByShards.get(shardId);
			ObjectPool<Client> pool = cliList.get(0);
			int ntry = 1;

			while (ntry < 3) {
				ntry ++;
				Poolable<Client> obj = null;
				try {
					obj = pool.borrowObject(true);
					Client client = obj.getObject();
					if (!client.getInputProtocol().getTransport().isOpen()) {
						client.getInputProtocol().getTransport().open();
					}
					GetDataResult result = client.getData(req);

					Map<Key, List <DataPoint>> map = new HashMap<Key, List <DataPoint>>();

					List<TimeSeriesData> lts = result.getResults();
					int idx = 0;
					for(TimeSeriesData ts: lts) {
						Key key = keys.get(idx);
						if (ts.status == StatusCode.OK) {
							List<TimeSeriesBlock> ltsb = ts.getData();
							List <DataPoint> dps  = new ArrayList<DataPoint>();
							for(TimeSeriesBlock tsb: ltsb) {
								BlockDecoder bd = new BlockDecoder(key, tsb);
								List <DataPoint> _dps = bd.readDps();
								dps.addAll(_dps);
							}
							map.put(key, dps);
						}
						idx++;
					}
					completableFuture.complete(map);
				} catch (TException e) {
					if (ntry == 3) {
						e.printStackTrace();
						completableFuture.complete(null);
					}
					try {
						Client client = obj.getObject();
						client.getInputProtocol().getTransport().close();
						client.getOutputProtocol().getTransport().close();
					} catch(Exception e1) {

					}
				} finally {
					if(null != obj) {
						pool.returnObject(obj);
					}
				}
			}
		});
		return completableFuture;
	}
	
	public CompletableFuture<List <DataPoint>> getDataPointsV2(long start, long end, List <Key> keys) {
		CompletableFuture<List<DataPoint>> completableFuture =  new CompletableFuture<List <DataPoint>>();
		executorReader.submit(() -> {
			GetDataRequest req = new GetDataRequest();

			req.setBegin(start);
			req.setEnd(end);
			req.setKeys(keys);

			long shardId = keys.get(0).getShardId();

			List<ObjectPool<Client>> cliList = clusterByShards.get(shardId);
			ObjectPool<Client> pool = cliList.get(0);
			int ntry = 1;

			while (ntry < 3) {
				ntry ++;
				Poolable<Client> obj = null;
				try {
					obj = pool.borrowObject(true);
					Client client = obj.getObject();
					if (!client.getInputProtocol().getTransport().isOpen()) {
						client.getInputProtocol().getTransport().open();
					}
					GetDataResult result = client.getData(req);

					List <DataPoint> dpsall = new ArrayList <DataPoint>();

					List<TimeSeriesData> lts = result.getResults();
					int idx = 0;
					for(TimeSeriesData ts: lts) {
						Key key = keys.get(idx);
						if (ts.status == StatusCode.OK) {
							List<TimeSeriesBlock> ltsb = ts.getData();
							List <DataPoint> dps  = new ArrayList<DataPoint>();
							for(TimeSeriesBlock tsb: ltsb) {
								BlockDecoder bd = new BlockDecoder(key, tsb);
								List <DataPoint> _dps = bd.readDps();
								dps.addAll(_dps);
							}
							dpsall.addAll(dps);
						}
						idx++;
					}
					completableFuture.complete(dpsall);
				} catch (TException e) {
					if (ntry == 3) {
						e.printStackTrace();
						completableFuture.complete(null);
					}
					try {
						Client client = obj.getObject();
						client.getInputProtocol().getTransport().close();
						client.getOutputProtocol().getTransport().close();
					} catch(Exception e1) {

					}
				} finally {
					if(null != obj) {
						pool.returnObject(obj);
					}
				}
			}
		});
		return completableFuture;
	}
}