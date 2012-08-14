package com.rubiconproject.oss.kv.distributed.hashing;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class HashRing<K, V> {
	private int nodeCount = 0;

	private TreeMap<K, V> map = new TreeMap<K, V>();

	public HashRing(int nodeCount) {
		this.nodeCount = nodeCount;
	}

	/**
	 * Return the node count. Node count is independent from the ring as is
	 * saved here merely as a convenience. It needs to be atomically assigned at
	 * the same time as the switch to a new hash ring.
	 * 
	 * @return
	 */
	public int getNodeCount() {
		return nodeCount;
	}

	/**
	 * Place the given key on the ring, returning the matching key/value pair.
	 * 
	 * @param key
	 * @return
	 */
	public Map.Entry<K, V> place(K key) {
		assert map.size() > 0;
		Map.Entry<K, V> entry = map.ceilingEntry(key);
		if (entry == null)
			entry = map.firstEntry();
		return entry;
	}

	/**
	 * Returns the number of key-value mappings in this map.
	 * 
	 * @return
	 */
	public int size() {
		return map.size();
	}

	/**
	 * Returns the value to which the specified key is mapped, or null if this
	 * map contains no mapping for the key.
	 * 
	 * @param key
	 * @return
	 */
	public V get(K key) {
		return map.get(key);
	}

	/**
	 * Associates the specified value with the specified key in this map.
	 * 
	 * @param key
	 * @param value
	 */
	public void put(K key, V value) {
		map.put(key, value);
	}

	/**
	 * Returns a key-value mapping associated with the least key in this map, or
	 * null if the map is empty.
	 * 
	 * @return
	 */
	public Map.Entry<K, V> firstEntry() {
		assert map.size() > 0;
		return map.firstEntry();
	}

	/**
	 * Returns a key-value mapping associated with the greatest key in this map,
	 * or null if the map is empty.
	 * 
	 * @return
	 */
	public Map.Entry<K, V> lastEntry() {
		assert map.size() > 0;
		return map.lastEntry();
	}

	/**
	 * Returns a key-value mapping associated with the greatest key strictly
	 * less than the given key, or null if there is no such key.
	 * 
	 * @param key
	 * @return
	 */
	public Map.Entry<K, V> lowerEntry(K key) {
		assert map.size() > 0;
		return map.lowerEntry(key);
	}

	/**
	 * Returns a key-value mapping associated with the least key strictly
	 * greater than the given key, or null if there is no such key.
	 * 
	 * @param key
	 * @return
	 */
	public Map.Entry<K, V> higherEntry(K key) {
		assert map.size() > 0;
		return map.higherEntry(key);
	}

	/**
	 * Returns a Collection view of the values contained in this map.
	 * 
	 * @return
	 */
	public Collection<V> values() {
		return map.values();
	}
}
