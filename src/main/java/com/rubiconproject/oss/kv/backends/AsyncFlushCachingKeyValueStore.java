package com.rubiconproject.oss.kv.backends;

import java.io.IOException;

import com.rubiconproject.oss.kv.AsyncFlushQueue;
import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

/**
 * An AsyncFlushCachingKeyValueStore is like a CachingKeyValueStore except that
 * write methods are expected to occur in a background thread. This class does
 * not dictate the actual implementation of the write queue, but delegates to an
 * AsyncFlushQueue.
 * 
 * @author sam
 * 
 */
public class AsyncFlushCachingKeyValueStore extends CachingKeyValueStore {

	public static final String IDENTIFIER = "asyncflushcaching";

	private AsyncFlushQueue queue;

	public AsyncFlushCachingKeyValueStore() {
		super();
	}

	public AsyncFlushCachingKeyValueStore(KeyValueStore master,
			KeyValueStore cache, AsyncFlushQueue queue) {
		super(master, cache);
		this.queue = queue;
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void set(String key, Object value)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		cache.set(key, value);
		if (queue != null)
			queue.set(key, value);
	}

	@Override
	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		cache.set(key, value, transcoder);
		if (queue != null)
			queue.set(key, value, transcoder);
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		cache.delete(key);
		if (queue != null)
			queue.delete(key);
	}
}
