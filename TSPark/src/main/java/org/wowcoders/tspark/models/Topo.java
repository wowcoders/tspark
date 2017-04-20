package org.wowcoders.tspark.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.wowcoders.tspark.utils.Combinations;
import org.wowcoders.tspark.utils.Hash;
import org.wowcoders.tspark.utils.Pair;

public class Topo implements Serializable, Cloneable {
	private static final long serialVersionUID = -4963561786934489036L;
	List<String>[] indexes = null;

	List<Pair<String, String>> tags = null;
	String calculatedHash = null;
	String calculatedHashDims = null;
	long cacheTime = -1;

	public Topo() {
		tags = new ArrayList<Pair<String, String>>();
		clearFlags();
	}

	public Topo(Topo t) {
		tags = t.tags;
		calculatedHash = t.calculatedHash;
		hashAndIndex();
	}

	public void clear() {
		tags.clear();
		clearFlags();
	}

	public void clearFlags() {
		calculatedHash = null;
		calculatedHashDims = null;
		indexes = null;
		cacheTime = -1;
	}

	public Topo(List<Pair<String, String>> tags) {
		setTags(tags);
	}

	public void add(String key, String value) {
		tags.add(new Pair<String, String>(key, value));
		clearFlags();
	}

	public void add(Pair<String, String> pair) {
		tags.add(pair);
		clearFlags();
	}

	public void sort() {
		Collections.sort(tags, (o1, o2) -> o1.first.compareTo(o2.first));
	}

	public String toString() {
		return topoString();
	}

	public String topoString() {
		StringBuilder sb = new StringBuilder();
		for(Pair<String, String> pair: tags) {
			sb.append("(");
			sb.append(pair.first);
			sb.append("=");
			sb.append(pair.second);
			sb.append(")");
		}
		return sb.toString();
	}

	private String _toStringDims() {
		StringBuilder sb = new StringBuilder();
		for(Pair<String, String> pair: tags) {
			sb.append("(");
			sb.append(pair.first);
			sb.append(")");
		}
		return sb.toString();
	}

	public String hash() {
		return _hash();
	}
	
	public String forceHash() {
		clearFlags();
		return _hash();
	}

	public void hashAndIndex() {
		if (calculatedHash == null) {
			_hash();
		}

		if (indexes == null) {
			indexes = getIndexes();
		}
	}

	public String _hash() {
		if (calculatedHash == null) {
			sort();
			String asString = topoString();
			calculatedHash = Hash.hash(asString);

			asString = _toStringDims();
			calculatedHashDims = Hash.hash(asString);
		}

		return calculatedHash;
	}

	public String hashTopoWithNoStar() {
		StringBuilder sb = new StringBuilder();
		for(Pair<String, String> pair: tags) {
			if (!pair.second.equals("*")) {
				sb.append("(");
				sb.append(pair.first);
				sb.append("=");
				sb.append(pair.second);
				sb.append(")");
			}
		}
		return Hash.hash(sb.toString());
	}

	public String _hashDims() {
		return calculatedHashDims;
	}

	public int hashCode() {
		String hash = _hash();
		//System.out.println("**"+hash);
		return hash.hashCode();
	}

	public List<Pair<String, String>> getTags() {
		return tags;
	}

	public void setTags(List<Pair<String, String>> tags) {
		this.tags = tags;
		clearFlags();
		hashAndIndex();
	}

	public List<String>[] getIndexes() {
		List<String> indexes1 = new ArrayList<String>();
		List<String> indexes2 = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		StringBuilder sbDims = new StringBuilder();
		for(int i = 1; i<= tags.size(); i++) {
			List<List<Pair<String, String>>> pairsList = Combinations.combination(tags, i);

			for(List<Pair<String, String>> pairs: pairsList) {
				for(Pair<String, String> pair: pairs) {
					sb.append("(");
					sb.append(pair.first);
					sb.append("=");
					sb.append(pair.second);
					sb.append(")");

					sbDims.append("(");
					sbDims.append(pair.first);
					sbDims.append(")");
				}

				String asString = sb.toString();
				String calculatedHash = Hash.hash(asString);
				indexes1.add(calculatedHash);

				asString = sbDims.toString();
				calculatedHash = Hash.hash(sbDims.toString());
				indexes2.add(calculatedHash);

				sb.setLength(0);
				sbDims.setLength(0);
			}
		}
		@SuppressWarnings("unchecked")
		List<String>[] indexSet = (ArrayList<String>[])new ArrayList[2];
		indexSet[0] = indexes1;
		indexSet[1] = indexes1;
		return indexSet;
	}

	public void setCacheTime() {
		cacheTime = System.currentTimeMillis()/1000;
	}

	public boolean updateRequired() {
		return System.currentTimeMillis()/1000 - cacheTime >= 604800;
	}


	public Topo clone() throws CloneNotSupportedException {
		List<Pair<String, String>> clone = new ArrayList<Pair<String, String>>(this.tags.size());
		for (Pair<String, String> item : this.tags) clone.add(item.clone());
		return new Topo(clone);
	}
}