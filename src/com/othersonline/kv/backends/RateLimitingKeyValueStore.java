package com.othersonline.kv.backends;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.util.RateLimiter;

public class RateLimitingKeyValueStore extends BaseManagedKeyValueStore
		implements KeyValueStore {
	public static final String IDENTIFIER = "ratelimiting";

	private KeyValueStore master;

	private RateLimiter readLimiter;

	private RateLimiter writeLimiter;

	public RateLimitingKeyValueStore() {
	}

	public RateLimitingKeyValueStore(KeyValueStore master,
			RateLimiter readLimiter, RateLimiter writeLimiter) {
		this.master = master;
		this.readLimiter = readLimiter;
		this.writeLimiter = writeLimiter;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void setMaster(KeyValueStore master) {
		this.master = master;
	}

	public void setReadRateLimiter(RateLimiter limiter) {
		this.readLimiter = limiter;
	}

	public void setWriteRateLimiter(RateLimiter limiter) {
		this.writeLimiter = limiter;
	}

	@Override
	public void start() throws IOException {
		super.start();
	}

	@Override
	public void stop() {
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		return master.exists(key);
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		return master.get(key);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return master.get(key, transcoder);
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return master.getBulk(keys);
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return master.getBulk(keys);
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return master.getBulk(keys, transcoder);
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		master.set(key, value);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		master.set(key, value, transcoder);
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		master.delete(key);
	}

	protected void assertWriteable() throws KeyValueStoreUnavailable {
		if (writeLimiter != null) {
			if (writeLimiter.allowNextEvent())
				writeLimiter.nextEvent();
			else
				throw new KeyValueStoreUnavailable();
		}
		super.assertWriteable();
	}

	protected void assertReadable() throws KeyValueStoreUnavailable {
		if (readLimiter != null) {
			if (readLimiter.allowNextEvent())
				readLimiter.nextEvent();
			else
				throw new KeyValueStoreUnavailable();
		}
		super.assertReadable();
	}

}
