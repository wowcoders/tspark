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
import com.facebook.beringei.thriftclient.BeringeiService;
import com.facebook.beringei.thriftclient.BeringeiService.AsyncClient;
import com.facebook.beringei.thriftclient.BeringeiService.AsyncClient.Factory;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

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

	Map<Long, List<ObjectPool<AsyncClient>>> clusterByShards = new ConcurrentHashMap<Long, List<ObjectPool<AsyncClient>>>();
	Map<Long, List<DataPoint>> batchByShards = new ConcurrentHashMap<Long, List<DataPoint>>();

	static TProtocolFactory proto_fac = new TProtocolFactory() {
		private static final long serialVersionUID = 1L;

		@Override
		public TProtocol getProtocol(TTransport trans) {
			return new TBinaryProtocol(trans);
		}
	};

	private ObjectFactory<AsyncClient> createObjectFactory(Pair<String, Integer> addr) {
		ObjectFactory<AsyncClient> factory = new ObjectFactory<AsyncClient>() {
			TAsyncClientManager asm = null;
			TNonblockingSocket tnbs = null;
			@Override public AsyncClient create() {
				Configuration cfg = Configuration.getInstnace();
				try {
					asm = new TAsyncClientManager();
				} catch (IOException e) {
					e.printStackTrace();
				}

				Factory fac = new AsyncClient.Factory(asm, proto_fac);
				try {
					tnbs = new TNonblockingSocket(addr.first, addr.second);
				} catch (IOException e) {
					e.printStackTrace();
				}
				tnbs.setTimeout((int)(cfg.getClientConfig().getConnectTimeout() 
						+ cfg.getClientConfig().getWriteTimeout()
						+ cfg.getClientConfig().getReadTimeout()));

				return fac.getAsyncClient(tnbs);
			}
			@Override public void destroy(AsyncClient o) {
				try {
					asm.stop();
					tnbs.close();
				} catch(Exception e) {

				} finally {
					asm = null;
					tnbs = null;
				}
			}
			@Override public boolean validate(AsyncClient o) {
				return asm != null && tnbs != null;
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
			ObjectPool<AsyncClient> cliPool = new ObjectPool<AsyncClient>(config, createObjectFactory(hostAddr));
			for(long i = startShard; i < shardCount; i++) {
				clusterByShards.put(i, new ArrayList<ObjectPool<AsyncClient>>() {{
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

			List<ObjectPool<AsyncClient>> cliList = clusterByShards.get(shardId);

			ObjectPool<AsyncClient> pool = cliList.get(0);
			Poolable<AsyncClient> obj = pool.borrowObject(true);
			AsyncClient client = obj.getObject();

			CompletableFuture<List <DataPoint>> _completableFuture = new CompletableFuture<List <DataPoint>>();
			PutDataPointsRequestResponseHandler pdprrh = new PutDataPointsRequestResponseHandler(req, _completableFuture);
			_completableFuture.thenAccept(rdps->{
				completableFuture.complete(rdps);
				pool.returnObject(obj);
			});
			pdprrh.on(client);
		});
		return completableFuture;
	}

	class PutDataPointsRequestResponseHandler  {
		PutDataRequest req;
		CompletableFuture<List <DataPoint>> cf;
		PutDataPointsRequestResponseHandler(PutDataRequest req, CompletableFuture<List <DataPoint>> cf) {
			this.req = req;
			this.cf = cf;
		}

		public void on(AsyncClient cli) {
			try {
				((BeringeiService.AsyncClient)cli).putDataPoints(req,  new AsyncMethodCallback<PutDataResult>() {
					@Override
					public void onError(Exception e) {
						e.printStackTrace();
						//taskDone();
					}

					@Override
					public void onComplete(PutDataResult result) {
						cf.complete(result.getData());
						//taskDone();
					}
				});
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private CompletableFuture<List<TimeSeriesData>> getData(AsyncClient client, long start, long end, List <Key> keys) {
		CompletableFuture<List<TimeSeriesData>> completableFuture =  new CompletableFuture<List<TimeSeriesData>>();

		GetDataRequest req = new GetDataRequest();

		req.setBegin(start);
		req.setEnd(end);
		req.setKeys(keys);

		GetDataPointsRequestResponseHandler gdprrh = new GetDataPointsRequestResponseHandler(req, completableFuture);
		gdprrh.on(client);
		// client.req(new GetDataPointsRequestResponseHandler(req, completableFuture));

		return completableFuture;
	}

	class GetDataPointsRequestResponseHandler {
		GetDataRequest req;
		CompletableFuture<List<TimeSeriesData>> cf;
		GetDataPointsRequestResponseHandler(GetDataRequest req, CompletableFuture<List<TimeSeriesData>> cf) {
			this.req = req;
			this.cf = cf;
		}

		public void on(AsyncClient cli) {
			try {
				((BeringeiService.AsyncClient)cli).getData(req,  new AsyncMethodCallback<GetDataResult>() {
					@Override
					public void onError(Exception e) {
						e.printStackTrace();
						//taskDone();
						cf.completeExceptionally(e);
					}

					@Override
					public void onComplete(GetDataResult result) {
						cf.complete(result.getResults());
					}
				});
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}

	public CompletableFuture<Map<Key, List <DataPoint>>> getDataPointsByKey(long start, long end, List <Key> keys) {
		CompletableFuture<Map<Key, List <DataPoint>>> completableFuture =  new CompletableFuture<Map<Key, List <DataPoint>>>();
		long shardId = keys.get(0).getShardId();

		List<ObjectPool<AsyncClient>> cliList = clusterByShards.get(shardId);
		ObjectPool<AsyncClient> pool = cliList.get(0);

		Poolable<AsyncClient> obj = pool.borrowObject(true);
		AsyncClient client = obj.getObject();

		CompletableFuture<List<TimeSeriesData>> cf = getData(client, start, end, keys);
		cf.thenAccept(lts -> {
			pool.returnObject(obj);
			executorReader.submit(() -> {;
			if (lts == null) {

			} else {

				Map<Key, List <DataPoint>> map = new HashMap<Key, List <DataPoint>>();

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
			}

			});
		}).exceptionally(tw-> {
			tw.printStackTrace();
			completableFuture.completeExceptionally(tw);
			return null;
		});

		return completableFuture;
	}

	public CompletableFuture<List <DataPoint>> getDataPoints(long start, long end, List <Key> keys) {
		CompletableFuture<List<DataPoint>> completableFuture =  new CompletableFuture<List <DataPoint>>();

		long shardId = keys.get(0).getShardId();

		List<ObjectPool<AsyncClient>> cliList = clusterByShards.get(shardId);
		ObjectPool<AsyncClient> pool = cliList.get(0);

		Poolable<AsyncClient> obj = cliList.get(0).borrowObject(true);
		AsyncClient client = obj.getObject();
		CompletableFuture<List<TimeSeriesData>> cf = getData(client, start, end, keys);
		cf.thenAccept(lts -> {
			pool.returnObject(obj);
			if (lts == null) {

			} else {
				executorReader.submit(() -> {
					List <DataPoint> dpsall = new ArrayList <DataPoint>();

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
				});
			}
		}).exceptionally(tw-> {
			completableFuture.completeExceptionally(tw);
			return null;
		});
		return completableFuture;
	}
}