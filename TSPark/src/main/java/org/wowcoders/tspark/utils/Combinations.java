package org.wowcoders.tspark.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Combinations {
	public static <T> List<List<T>> combination(List<T> values, int size) {
		if (0 == size) {
			return Collections.singletonList(Collections.<T> emptyList());
		}

		if (values.isEmpty()) {
			return Collections.emptyList();
		}

		List<List<T>> combination = new LinkedList<List<T>>();

		T actual = values.iterator().next();

		List<T> subSet = new LinkedList<T>(values);
		subSet.remove(actual);

		List<List<T>> subSetCombination = combination(subSet, size - 1);

		for (List<T> set : subSetCombination) {
			List<T> newSet = new LinkedList<T>(set);
			newSet.add(0, actual);
			combination.add(newSet);
		}

		combination.addAll(combination(subSet, size));

		return combination;
	}

	public static void main(String []args) {
		List <Pair<String, String>> tags = new ArrayList<Pair<String, String>>();
		tags.add(new Pair<String, String>("pool", "login"));
		tags.add(new Pair<String, String>("colo", "lvs"));
		tags.add(new Pair<String, String>("host", "login00001"));
		
		Collections.sort(tags, (o1, o2) -> o1.first.compareTo(o2.first));

		for(int i = 1; i<= tags.size(); i++) {
			List<List<Pair<String, String>>> s = combination(tags, i);
			System.out.println(s);
		}
	}
}