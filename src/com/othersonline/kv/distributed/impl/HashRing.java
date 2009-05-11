package com.othersonline.kv.distributed.impl;

import java.util.Map;
import java.util.TreeMap;

public class HashRing<K, V> {
	private TreeMap<K, V> map = new TreeMap<K, V>();

	public int size() {
		return map.size();
	}

	public V get(K key) {
		return map.get(key);
	}

	public void put(K key, V value) {
		map.put(key, value);
	}

	public Map.Entry<K, V> place(K key) {
		assert map.size() > 0;
		Map.Entry<K, V> entry = map.ceilingEntry(key);
		if (entry == null)
			entry = map.firstEntry();
		return entry;
	}

	public Map.Entry<K, V> firstEntry() {
		assert map.size() > 0;
		return map.firstEntry();
	}

	public Map.Entry<K, V> lastEntry() {
		assert map.size() > 0;
		return map.lastEntry();
	}

	public Map.Entry<K, V> lowerEntry(K key) {
		assert map.size() > 0;
		return map.lowerEntry(key);
	}

	public Map.Entry<K, V> higherEntry(K key) {
		assert map.size() > 0;
		return map.higherEntry(key);
	}
}
