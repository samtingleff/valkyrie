package com.othersonline.kv.mgmt;

import java.io.IOException;

public interface MemcachedMXBean {

	public void start() throws IOException;

	public void stop();

	public String getStatus();

	public void offline();

	public void readOnly();

	public void online();

	public long getTotalObjectCount();
	
	public long getTotalByteCount();
	
	public long getTotalEvictions();
	
	public double getHitRatio();
}