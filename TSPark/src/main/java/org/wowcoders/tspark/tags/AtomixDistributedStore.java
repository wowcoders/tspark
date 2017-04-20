package org.wowcoders.tspark.tags;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wowcoders.tspark.utils.Pair;

import io.atomix.Atomix;
import io.atomix.AtomixReplica;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.collections.DistributedMap;
import io.atomix.collections.DistributedMultiMap;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

public class AtomixDistributedStore {
	final static Logger logger = LoggerFactory.getLogger(AtomixDistributedStore.class);
	 
	static AtomixReplica atomix = null;
	
	public static DistributedMultiMap<Object, Object> map2 = null;
	public static DistributedMultiMap<Object, Object> map3 = null;

	public static DistributedMultiMap<Object, Object> topoKeyToShardIds = null;
	
	public static DistributedMap<Object, Object> topoMap = null;
	public static DistributedMap<Object, Object> metrics = null;
	public static DistributedMap<Object, Object> tagv = null;
	public static DistributedMap<Object, Object> tagk = null;

	
	private static void addShutdownHooks() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					if (atomix != null) {
						System.out.println("Shutting down");
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		});
	}

	public static CompletableFuture<AtomixReplica> start(
			Pair<String, Integer> listen, List<Pair<String, Integer>> clusterInp) {
		addShutdownHooks();

		List<Address> cluster = new ArrayList<Address>();
		
		if (clusterInp != null) {
			clusterInp.stream().forEach(addr-> {
				if (!(listen.first.equals(addr.first) &&  listen.second.equals(addr.second))) {
				cluster.add(new Address(addr.first, addr.second));
				}
			});
		}
		
		Address address = null;
		address = new Address(listen.first, listen.second);

		String dirName = "./atomix-data/";

		atomix = AtomixReplica.builder(address)
				.withType(AtomixReplica.Type.ACTIVE)
				.withTransport(NettyTransport.builder().withThreads(4).build())
				.withSerializer(new Serializer().disableWhitelist())
				.withStorage(Storage.builder()
						.withDirectory(new File(dirName))
						.withStorageLevel(StorageLevel.MAPPED)
						.build()).build();

		CompletableFuture<AtomixReplica> cf = atomix.bootstrap(cluster);
		Thread t = new Thread() {
			@Override
			public void run() {
				while (!cf.isDone()) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						break;
					}
				}
				if (cf.isDone()) {
					metrics = atomix.getMap("beringei-metrics").join();
					tagk = atomix.getMap("beringei-tagk").join();
					tagv = atomix.getMap("beringei-tagv").join();
					
					topoMap = atomix.getMap("beringei-topo-key-to-topo-map").join();
					topoKeyToShardIds = atomix.getMultiMap("beringei-topo-key-to-shard-ids").join();
					map2 = atomix.getMultiMap("beringei-metrickey-to-dimskey-map").join();
					map3 = atomix.getMultiMap("beringei-topokey-dimskey-metrickey-to-keys-map").join();
					
					logger.info("Got the atomix map references.");
				}
			}
		};
		t.start();
		return cf;
	}

	public static Atomix getInstance() {
		return atomix;
	}

	public static void main(String []args) throws Throwable {
		start(new Pair<String, Integer>("0.0.0.0", 41292), null).join();
	}
}
