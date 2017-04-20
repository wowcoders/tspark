package org.wowcoders.tspark.tags;

import java.util.concurrent.CompletableFuture;

public interface WriterInterface {
	CompletableFuture<Boolean> bulkUpsert();
	
	//write short topokey with dims key available to rowkey
	//find the topokeys tied to the metric
	//store values
	//store keys
	//api_aggregators
	//api_config_filters
	//api_query
	//rowkey to -> topo
	//rowkey to -> topo multimap
}