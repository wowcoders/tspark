package org.wowcoders.tspark.tags;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.atomix.Atomix;
import io.atomix.collections.DistributedMap;

public class AtomixWriter implements WriterInterface {
	@Override
	public CompletableFuture<Boolean> bulkUpsert() {
		CompletableFuture<Boolean> cf = new CompletableFuture<Boolean>();
		Atomix atomix = AtomixDistributedStore.getInstance();
		DistributedMap<Object, Object> map = atomix.getMap("beringei-topo").join();
		map.put("",
				"",
				Duration.ofDays(8));
		return cf;
	}
}