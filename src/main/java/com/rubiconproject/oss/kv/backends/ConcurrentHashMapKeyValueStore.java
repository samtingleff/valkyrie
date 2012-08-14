package com.rubiconproject.oss.kv.backends;

import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.rubiconproject.oss.kv.BaseManagedKeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.annotations.Configurable;
import com.rubiconproject.oss.kv.annotations.Configurable.Type;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

/**
 * A simple in-memory kv store based on {@link ConcurrentHashMap}. Useful for testing or debugging, particularly to simulate slow or non-responsive backends.
 * 
 * Simulating a non-responsive backend:
 *  - use a uri like 'hash://hash?writeSleepTime=800&readSleepTime=100'
 *  - will write after sleeping for 800ms and return from reads after sleeping for 100ms
 * 
 * @author stingleff
 *
 */
public class ConcurrentHashMapKeyValueStore extends BaseManagedKeyValueStore
		implements KeyValueStore {
	public static final String IDENTIFIER = "hashtable";

	private Map<String, Object> map = new ConcurrentHashMap<String, Object>();

	private long writeSleepTime = -1;

	private long readSleepTime = -1;

	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Configurable(name = "writeSleepTime", accepts = Type.LongType)
	public void setWriteSleepTime(long delay) {
		this.writeSleepTime = delay;
	}

	@Configurable(name = "readSleepTime", accepts = Type.LongType)
	public void setReadSleepTime(long delay) {
		this.readSleepTime = delay;
	}

	public void stop() {
		map.clear();
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		return map.containsKey(key);
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		if (readSleepTime > 0)
			sleep(readSleepTime);
		return map.get(key);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		if (readSleepTime > 0)
			sleep(readSleepTime);
		return map.get(key);
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		if (readSleepTime > 0)
			sleep(readSleepTime);
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public Map<String, Object> getBulk(final List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		if (readSleepTime > 0)
			sleep(readSleepTime);
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public Map<String, Object> getBulk(final List<String> keys,
			Transcoder transcoder) throws KeyValueStoreException, IOException {
		assertReadable();
		if (readSleepTime > 0)
			sleep(readSleepTime);
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key, transcoder);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		if (writeSleepTime > 0)
			sleep(writeSleepTime);
		assertWriteable();
		map.put(key, value);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		if (writeSleepTime > 0)
			sleep(writeSleepTime);
		map.put(key, value);
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		if (writeSleepTime > 0)
			sleep(writeSleepTime);
		map.remove(key);
	}

	private void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
		}
	}
}
