package org.wowcoders.tspark.tags;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.wowcoders.tspark.models.Topo;

public interface ReaderInterface {
	CompletableFuture<Boolean> bulkRead();

	//find topo matching key
	//short topokey with dims key available
	//find the topokeys tied to the metric
	
	public CompletableFuture<List<String>> findKeys(Topo t);
}