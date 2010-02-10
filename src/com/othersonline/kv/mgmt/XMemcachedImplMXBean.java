package com.othersonline.kv.mgmt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.backends.XMemcachedKeyValueStore;

public class XMemcachedImplMXBean implements MemcachedMXBean {
	private XMemcachedKeyValueStore mcc;

	private BaseKeyValueStoreImplMXBean delegate;

	public XMemcachedImplMXBean(KeyValueStore store) {
		this.mcc = (XMemcachedKeyValueStore) store;
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
		Map<InetSocketAddress, Map<String, String>> stats = mcc.getStats();
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
