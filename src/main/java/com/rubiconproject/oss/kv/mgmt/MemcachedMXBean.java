package com.rubiconproject.oss.kv.mgmt;

import java.io.IOException;

import com.rubiconproject.oss.kv.KeyValueStoreException;

public interface MemcachedMXBean {
	public void start() throws IOException;

	public void stop();

	public String getStatus();

	public void offline();

	public void readOnly();

	public void online();

	public long getTotalObjectCount() throws KeyValueStoreException;

	public long getTotalByteCount() throws KeyValueStoreException;

	public long getTotalEvictions() throws KeyValueStoreException;

	public double getHitRatio() throws KeyValueStoreException;
}