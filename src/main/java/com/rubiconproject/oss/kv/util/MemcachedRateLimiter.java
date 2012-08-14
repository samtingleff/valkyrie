package com.rubiconproject.oss.kv.util;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.backends.MemcachedKeyValueStore;

public class MemcachedRateLimiter implements RateLimiter {

	private MemcachedKeyValueStore mcc;

	private String counterKey;

	private long exp;

	private long maxEvents;

	private boolean throwExceptionOnErrors = false;

	public MemcachedRateLimiter() {
		// create a psuedo-random string for the counter key
		Random r = new Random();
		this.counterKey = "rate-limit-" + r.nextInt(Integer.MAX_VALUE);
	}

	public MemcachedRateLimiter(MemcachedKeyValueStore mcc) {
		this();
		this.mcc = mcc;
	}

	public MemcachedRateLimiter(MemcachedKeyValueStore mcc, String counterKey) {
		this();
		this.mcc = mcc;
		this.counterKey = counterKey;
	}

	public void setMemcached(MemcachedKeyValueStore mcc) {
		this.mcc = mcc;
	}

	public void setLimit(TimeUnit timeUnit, long count, long maxEvents) {
		// convert units * count to # of seconds for our expire time
		this.exp = TimeUnit.SECONDS.convert(count, timeUnit);
		this.maxEvents = maxEvents;
	}

	public boolean allowNextEvent() {
		return (getCounter() < maxEvents);
	}

	public void nextEvent() {
		try {
			long count = mcc.incr(counterKey, 1, 1, (int) exp);
		} catch (KeyValueStoreException e) {
			if (throwExceptionOnErrors)
				throw new RuntimeException(e);
		}
	}

	public long getCounter() {
		long count = 0;
		try {
			String s = (String) mcc.get(counterKey);
			if (s != null) {
				count = Long.parseLong(s);
			}
		} catch (KeyValueStoreException e) {
			if (throwExceptionOnErrors)
				throw new RuntimeException(e);
		} catch (IOException e) {
			if (throwExceptionOnErrors)
				throw new RuntimeException(e);
		}
		return count;
	}

}
