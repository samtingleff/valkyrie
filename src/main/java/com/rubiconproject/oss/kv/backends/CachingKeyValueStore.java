package com.rubiconproject.oss.kv.backends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rubiconproject.oss.kv.BaseManagedKeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.annotations.Configurable;
import com.rubiconproject.oss.kv.annotations.Configurable.Type;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

public class CachingKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "caching";

	protected Log log = LogFactory.getLog(getClass());

	protected KeyValueStore master;

	protected KeyValueStore cache;

	protected boolean cacheOnMiss = true;

	protected boolean cacheOnSet = true;

	public CachingKeyValueStore() {
	}

	public CachingKeyValueStore(KeyValueStore master, KeyValueStore cache) {
		this.master = master;
		this.cache = cache;
	}

	public void setMaster(KeyValueStore master) {
		this.master = master;
	}

	public void setCache(KeyValueStore cache) {
		this.cache = cache;
	}

	@Configurable(name = "cacheOnMiss", accepts = Type.BooleanType)
	public void setCacheOnMiss(boolean cacheOnMiss) {
		this.cacheOnMiss = cacheOnMiss;
	}

	@Configurable(name = "cacheOnSet", accepts = Type.BooleanType)
	public void setCacheOnSet(boolean cacheOnSet) {
		this.cacheOnSet = cacheOnSet;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		super.start();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		boolean b = false;
		try {
			b = cache.exists(key);
		} catch (Exception e) {
			log.warn("Unable to call exists() on cache: " + e.getMessage());
		}
		if (!b)
			b = master.exists(key);
		return b;
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		Object obj = null;
		try {
			obj = cache.get(key);
		} catch (Exception e) {
			log.warn("Unable to call get() on cache: " + e.getMessage());
		}
		if (obj == null) {
			obj = master.get(key);
			if ((obj != null) && (cacheOnMiss)) {
				try {
					cache.set(key, (Object) obj);
				} catch (Exception e) {
					log
							.warn("Unable to call set() on cache: "
									+ e.getMessage());
				}
			}
		}
		return obj;
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Object obj = null;
		try {
			obj = cache.get(key, transcoder);
		} catch (Exception e) {
			log.warn("Unable to call get() on cache: " + e.getMessage());
		}
		if (obj == null) {
			obj = master.get(key, transcoder);
			if ((obj != null) && (cacheOnMiss)) {
				try {
					cache.set(key, (Object) obj, transcoder);
				} catch (KeyValueStoreException e) {
					if ((e.getCause() != null)
							&& (e.getCause().getClass()
									.equals(TimeoutException.class))) {
						log
								.warn("Unable to call set() on cache due to TimeoutException: "
										+ e.getMessage());
					} else {
						log.warn("Unable to call set() on cache", e);
					}
				} catch (Exception e) {
					log.warn("Unable to call set() on cache", e);
				}
			}
		}
		return obj;
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		List<String> coll = Arrays.asList(keys);
		return getBulk(coll);
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		try {
			Map<String, Object> cached = cache.getBulk(keys);
			results.putAll(cached);
		} catch (Exception e) {
			log.warn("Unable to call getBulk() on cache: " + e.getMessage());
		}
		if (results.size() < keys.size()) {
			// find all keys not in cache
			List<String> backendQueryKeys = new ArrayList<String>(keys.size()
					- results.size());
			for (String key : keys) {
				if (!results.containsKey(key))
					backendQueryKeys.add(key);
			}
			Map<String, Object> backendResults = master
					.getBulk(backendQueryKeys);
			results.putAll(backendResults);
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		try {
			Map<String, Object> cached = cache.getBulk(keys, transcoder);
			results.putAll(cached);
		} catch (Exception e) {
			log.warn("Unable to call getBulk() on cache: " + e.getMessage());
		}
		if (results.size() < keys.size()) {
			// find all keys not in cache
			List<String> backendQueryKeys = new ArrayList<String>(keys.size()
					- results.size());
			for (String key : keys) {
				if (!results.containsKey(key))
					backendQueryKeys.add(key);
			}
			Map<String, Object> backendResults = master.getBulk(
					backendQueryKeys, transcoder);
			results.putAll(backendResults);
		}
		return results;
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		try {
			if (cacheOnSet) {
				cache.set(key, value);
			} else
				cache.delete(key);
		} catch (Exception e) {
			log.warn("Unable to call set() on cache: " + e.getMessage());
		}
		master.set(key, value);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		try {
			if (cacheOnSet)
				cache.set(key, value, transcoder);
			else
				cache.delete(key);
		} catch (Exception e) {
			log.warn("Unable to call set() on cache: " + e.getMessage());
		}
		master.set(key, value, transcoder);
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		try {
			cache.delete(key);
		} catch (Exception e) {
			log.warn("Unable to call delete() on cache: " + e.getMessage());
		}
		master.delete(key);
	}

}
