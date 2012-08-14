package com.othersonline.kv.util;

import java.util.concurrent.TimeUnit;

/**
 * A simple, in jvm rate limiter that allows x events per time period.
 * 
 * @author sam
 * 
 */
public class SimpleRateLimiter implements RateLimiter {
	private long lastTimerReset = System.currentTimeMillis();

	private long max = 0;

	private long period = 0;

	private long counter = 0;

	public SimpleRateLimiter() {
	}

	public SimpleRateLimiter(TimeUnit timeUnit, long count, long maxEvents) {
		this.period = timeUnit.toMillis(count);
		this.max = maxEvents;
	}

	public void setLimit(TimeUnit timeUnit, long count, long maxEvents) {
		this.period = timeUnit.toMillis(count);
		this.max = maxEvents;
	}

	public boolean allowNextEvent() {
		resetTimer();
		boolean allow = (counter + 1 <= max);
		return allow;
	}

	public void nextEvent() {
		resetTimer();
		++counter;
	}

	public long getCounter() {
		resetTimer();
		return counter;
	}

	private void resetTimer() {
		long now = System.currentTimeMillis();
		if ((now - lastTimerReset) > period) {
			lastTimerReset = now;
			counter = 0;
		}
	}
}
