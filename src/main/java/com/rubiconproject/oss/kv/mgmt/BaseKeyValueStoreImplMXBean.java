package com.rubiconproject.oss.kv.mgmt;

import java.io.IOException;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreStatus;

public class BaseKeyValueStoreImplMXBean implements KeyValueStoreMXBean {
	private KeyValueStore store;

	public BaseKeyValueStoreImplMXBean(KeyValueStore store) {
		this.store = store;
	}

	public void start() throws IOException {
		store.start();
	}

	public void stop() {
		store.stop();
	}

	public String getStatus() {
		return store.getStatus().toString().toLowerCase();
	}

	public void offline() {
		store.setStatus(KeyValueStoreStatus.Offline);
	}

	public void readOnly() {
		store.setStatus(KeyValueStoreStatus.ReadOnly);
	}

	public void online() {
		store.setStatus(KeyValueStoreStatus.Online);
	}

}
