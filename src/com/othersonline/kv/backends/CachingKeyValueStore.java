package com.othersonline.kv.backends;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.transcoder.Transcoder;

public class CachingKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "caching";

	protected Log log = LogFactory.getLog(getClass());

	protected KeyValueStore master;

	protected KeyValueStore cache;

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

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void start() throws IOException {
		super.start();
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		boolean b = false;
		try {
			b = cache.exists(key);
		} catch (Exception e) {
			log.warn("Unable to call exists() on cache", e);
		}
		if (!b)
			b = master.exists(key);
		return b;
	}

	@Override
	public Object get(String key) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		assertReadable();
		Object obj = null;
		try {
			obj = cache.get(key);
		} catch (Exception e) {
			log.warn("Unable to call get() on cache", e);
		}
		if (obj == null)
			obj = master.get(key);
		return obj;
	}

	@Override
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Object obj = null;
		try {
			obj = cache.get(key, transcoder);
		} catch (Exception e) {
			log.warn("Unable to call get() on cache", e);
		}
		if (obj == null)
			obj = master.get(key, transcoder);
		return obj;
	}

	@Override
	public void set(String key, Serializable value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		try {
			cache.set(key, value);
		} catch (Exception e) {
			log.warn("Unable to call set() on cache", e);
		}
		master.set(key, value);
	}

	@Override
	public void set(String key, Serializable value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		try {
			cache.set(key, value, transcoder);
		} catch (Exception e) {
			log.warn("Unable to call set() on cache", e);
		}
		master.set(key, value, transcoder);
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		try {
			cache.delete(key);
		} catch (Exception e) {
			log.warn("Unable to call delete() on cache", e);
		}
		master.delete(key);
	}

}
