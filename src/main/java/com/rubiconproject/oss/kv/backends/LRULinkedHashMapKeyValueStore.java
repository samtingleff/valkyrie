package com.rubiconproject.oss.kv.backends;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.rubiconproject.oss.kv.BaseManagedKeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.annotations.Configurable;
import com.rubiconproject.oss.kv.annotations.Configurable.Type;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

public class LRULinkedHashMapKeyValueStore extends BaseManagedKeyValueStore
		implements KeyValueStore {
	private static final String IDENTIFIER = "lrulinkedhashmap";

	private LinkedHashMap map;

	private int initialCapacity = 100;

	private int max = 100;

	private float loadFactor = 0.75f;

	@Configurable(name = "max", accepts = Type.IntType)
	public void setInitialCapacity(int initialCapacity) {
		this.initialCapacity = initialCapacity;
	}

	@Configurable(name = "max", accepts = Type.IntType)
	public void setMax(int max) {
		this.max = max;
	}

	@Configurable(name = "loadFactor", accepts = Type.FloatType)
	public void setLoadFactory(float loadFactor) {
		this.loadFactor = loadFactor;
	}

	@Override
	public void start() throws IOException {
		map = new LinkedHashMap(initialCapacity, loadFactor, true) {
			protected boolean removeEldestEntry(Map.Entry entry) {
				return size() > max;
			}
		};
		super.start();
	}

	@Override
	public void stop() {
		super.stop();
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		return map.containsKey(key);
	}

	@Override
	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		return map.get(key);
	}

	@Override
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return get(key);
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object value = map.get(key);
			if (value != null)
				results.put(key, value);
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object value = map.get(key);
			if (value != null)
				results.put(key, value);
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return getBulk(keys);
	}

	@Override
	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		map.put(key, value);
	}

	@Override
	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		set(key, value);
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		map.remove(key);
	}

}
