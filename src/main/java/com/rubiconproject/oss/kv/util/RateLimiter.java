package com.rubiconproject.oss.kv.util;

import java.util.concurrent.TimeUnit;

/**
 * A rate limiter allows or denies an event based on how frequently that event
 * occurs.
 * 
 * @author sam
 * 
 */
public interface RateLimiter {
	public void setLimit(TimeUnit timeUnit, long count, long maxEvents);

	public boolean allowNextEvent();

	public void nextEvent();

	public long getCounter();
}
