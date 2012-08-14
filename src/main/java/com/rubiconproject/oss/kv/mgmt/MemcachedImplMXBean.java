package com.rubiconproject.oss.kv.mgmt;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.backends.MemcachedKeyValueStore;

public class MemcachedImplMXBean implements MemcachedMXBean {
	private MemcachedKeyValueStore mcc;

	private BaseKeyValueStoreImplMXBean delegate;

	public MemcachedImplMXBean(KeyValueStore store) {
		this.mcc = (MemcachedKeyValueStore) store;
		delegate = new BaseKeyValueStoreImplMXBean(store);
	}

	public void start() throws IOException {
		delegate.start();
	}

	public void stop() {
		delegate.stop();
	}

	public String getStatus() {
		return delegate.getStatus();
	}

	public void offline() {
		delegate.offline();
	}

	public void readOnly() {
		delegate.readOnly();
	}

	public void online() {
		delegate.online();
	}

	public long getTotalObjectCount() throws KeyValueStoreException {
		return getStatSum("curr_items");
	}

	public long getTotalByteCount() throws KeyValueStoreException {
		return getStatSum("bytes");
	}

	public long getTotalEvictions() throws KeyValueStoreException {
		return getStatSum("evictions");
	}

	public double getHitRatio() throws KeyValueStoreException {
		long getHits = getStatSum("get_hits");
		long getMisses = getStatSum("get_misses");
		long totalGets = getHits + getMisses;
		return ((double) getHits) / ((double) totalGets);
	}

	private long getStatSum(String stat) throws KeyValueStoreException {
		Map<SocketAddress, Map<String, String>> stats = mcc.getStats();
		// getStats("curr_items") is throwing an exception
		long total = 0;
		for (Map<String, String> map : stats.values()) {
			String s = map.get(stat);
			if (s != null) {
				try {
					long l = Long.parseLong(s);
					total += l;
				} catch (Exception e) {
				}
			}
		}
		return total;
	}
}
