package org.wowcoders.tspark.tags;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.wowcoders.tspark.models.Topo;

import io.atomix.Atomix;
import io.atomix.collections.DistributedMultiMap;

public class AtomixReader {
	public CompletableFuture<List<String>> findKeys(Topo t) {
		CompletableFuture<List<String>> cf = new CompletableFuture<List<String>>();
		
		Atomix atomix = AtomixDistributedStore.getInstance();

		DistributedMultiMap<Object, Object> map1 = atomix.getMultiMap("beringei-topo-key-to-keys-map").join();
		DistributedMultiMap<Object, Object> map2 = atomix.getMultiMap("beringei-topo-dimskey-to-keys-map").join();
		
		map1.get(t.hash()).thenAcceptAsync(_list1-> {
			if (_list1 != null) {
				map2.get(t.hash()).thenAcceptAsync(_list2-> {
					if (_list2 != null) {
						List<String> intersect = _list1.stream()
				                .filter(_list2::contains)
				                .map(s -> (String)s)
				                .collect(Collectors.toList());
						cf.complete(intersect);
					}
				}).exceptionally(tw -> {
					cf.completeExceptionally(tw);
					return null;
				});
			} 
		}).exceptionally(tw -> {
			cf.completeExceptionally(tw);
			return null;
		});

		return cf;
	}
}