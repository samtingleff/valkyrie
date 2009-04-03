package com.othersonline.kv.backends;

import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.transcoder.Transcoder;

public class HashtableKeyValueStore extends BaseManagedKeyValueStore implements
		KeyValueStore {
	public static final String IDENTIFIER = "hashtable";

	private Map<String, Object> map = new Hashtable<String, Object>();

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void stop() {
		map.clear();
		super.stop();
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
		return map.get(key);
	}

	@Override
	public void set(String key, Serializable value)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		map.put(key, value);
	}

	@Override
	public void set(String key, Serializable value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		map.put(key, value);
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		map.remove(key);
	}

}
