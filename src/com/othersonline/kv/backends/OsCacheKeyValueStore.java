package com.othersonline.kv.backends;

import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.opensymphony.oscache.base.CacheEntry;
import com.opensymphony.oscache.base.NeedsRefreshException;
import com.opensymphony.oscache.general.GeneralCacheAdministrator;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.transcoder.Transcoder;

public class OsCacheKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "oscache";

	private GeneralCacheAdministrator cache;

	private boolean memoryCache = true;

	private String cacheAlgorithm = "com.opensymphony.oscache.base.algorithm.LRUCache";

	private int cacheCapacity = 10000;

	private int refreshPeriod = CacheEntry.INDEFINITE_EXPIRY;

	private boolean diskPersistence = false;

	private boolean diskPersistenceOverflowOnly = true;

	private String diskPersistenceClass = "com.opensymphony.oscache.plugins.diskpersistence.DiskPersistenceListener";

	private String diskPersistencePath = null;

	public OsCacheKeyValueStore() {
	}

	public void setMemoryCache(boolean enable) {
		this.memoryCache = enable;
	}

	public void cacheAlgorithm(String algo) {
		this.cacheAlgorithm = algo;
	}

	public void setCapacity(int capacity) {
		this.cacheCapacity = capacity;
	}

	public void setRefreshPeriod(int refreshPeriod) {
		this.refreshPeriod = refreshPeriod;
	}

	public void setDiskPersistence(boolean diskPersistence) {
		this.diskPersistence = diskPersistence;
	}

	public void setDiskPersistenceOverflowOnly(
			boolean diskPersistenceOverflowOnly) {
		this.diskPersistenceOverflowOnly = diskPersistenceOverflowOnly;
	}

	public void setDiskPersistenceClass(String cls) {
		this.diskPersistenceClass = cls;
	}

	public void setDiskPersistencePath(String diskPersistencePath) {
		this.diskPersistencePath = diskPersistencePath;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		Properties props = new Properties();
		props.put("cache.memory", Boolean.toString(memoryCache));
		props.put("cache.algorithm", cacheAlgorithm);
		props.put("cache.capacity", cacheCapacity);
		props.put("cache.blocking", Boolean.toString(false));
		if (diskPersistence) {
			props.put("cache.persistence.overflow.only", Boolean
					.toString(diskPersistenceOverflowOnly));
			props.put("cache.persistence.class", diskPersistenceClass);
			props.put("cache.path", diskPersistencePath);
		}
		cache = new GeneralCacheAdministrator(props);
		super.start();
	}

	public void stop() {
		cache.destroy();
		cache = null;
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		boolean result = false;
		try {
			Object o = cache.getFromCache(key, refreshPeriod);
			result = (o == null) ? false : true;
		} catch (NeedsRefreshException nre) {
			cache.cancelUpdate(key);
		}
		return result;
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		Object o = null;
		try {
			o = cache.getFromCache(key, refreshPeriod);
		} catch (NeedsRefreshException nre) {
			cache.cancelUpdate(key);
		}
		return o;
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		return get(key);
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public Map<String, Object> getBulk(final List<String> keys)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public Map<String, Object> getBulk(final List<String> keys,
			Transcoder transcoder) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key, transcoder);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public void set(String key, Object value)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		boolean updated = false;
		try {
			cache.putInCache(key, value);
			updated = true;
		} finally {
			if (!updated)
				cache.cancelUpdate(key);
		}
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		set(key, value);
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		boolean updated = false;
		try {
			cache.flushEntry(key);
			updated = true;
		} finally {
			if (!updated)
				cache.cancelUpdate(key);
		}
	}
}
