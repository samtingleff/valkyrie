package com.rubiconproject.oss.kv.backends;

import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.Configuration;

import com.rubiconproject.oss.kv.BaseManagedKeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.annotations.Configurable;
import com.rubiconproject.oss.kv.annotations.Configurable.Type;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

public class EhCacheKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "ehcache";

	private String cacheName = "ehcache";

	private int capacity = 5000;
	
	private long capacityBytes = 0; 

	private int timeToLiveSeconds = 60;

	private int timeToIdleSeconds = 60;

	private CacheManager mgr;

	private Cache cache;

	public EhCacheKeyValueStore() {
	}

	@Configurable(name = "cacheName", accepts = Type.StringType)
	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}

	@Configurable(name = "cacheCapacity", accepts = Type.IntType)
	public void setCacheCapacity(int capacity) {
		this.capacity = capacity;
	}
	
	@Configurable(name = "cacheHeapBytes", accepts = Type.LongType)
	public void setCacheHeapBytes(long bytes) {
	    this.capacityBytes = bytes;
	}

	@Configurable(name = "timeToLive", accepts = Type.IntType)
	public void setTimeToLive(int timeToLiveSeconds) {
		this.timeToLiveSeconds = timeToLiveSeconds;
	}

	@Configurable(name = "timeToIdle", accepts = Type.IntType)
	public void setTimeToIdle(int timeToIdleSeconds) {
		this.timeToIdleSeconds = timeToIdleSeconds;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
	    if (capacityBytes == 0) {
	        mgr = CacheManager.create();
	    } else {
	        // Default value in default config is also 0,
	        // but might as well play it safe
	        Configuration managerConfig = new Configuration();
	        managerConfig.setMaxBytesLocalHeap(capacityBytes);
	        mgr = CacheManager.create(managerConfig);
	    }
	        
		cache = new Cache(cacheName, capacity, false, false, timeToLiveSeconds,
				timeToIdleSeconds);
		mgr.addCache(cache);
		super.start();
	}

	public void stop() {
		CacheManager.getInstance().shutdown();
		cache = null;
		mgr = null;
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		boolean result = false;
		Element el = cache.get(key);
		result = (el == null) ? false : true;
		return result;
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		Element el = cache.get(key);
		return (el == null) ? null : el.getObjectValue();
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		return get(key);
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
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
		assertWriteable();
		cache.put(new Element(key, value));
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		set(key, value);
	}

	public void set(String key, Object value, int timeToIdleSeconds,
			int timeToLiveSeconds) throws KeyValueStoreException, IOException {
		assertWriteable();
		cache.put(new Element(key, value, Boolean.FALSE, timeToIdleSeconds,
				timeToLiveSeconds));
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		cache.remove(key);
	}
}
