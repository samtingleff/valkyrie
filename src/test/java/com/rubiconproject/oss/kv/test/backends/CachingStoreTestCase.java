package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.KeyValueStoreStatus;
import com.rubiconproject.oss.kv.backends.CachingKeyValueStore;
import com.rubiconproject.oss.kv.backends.ConcurrentHashMapKeyValueStore;
import com.rubiconproject.oss.kv.backends.MemcachedKeyValueStore;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class CachingStoreTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		ConcurrentHashMapKeyValueStore master = new ConcurrentHashMapKeyValueStore();
		master.start();

		MemcachedKeyValueStore cache = new MemcachedKeyValueStore();
		cache.setHosts("dev-db:11211");
		cache.start();

		CachingKeyValueStore store = new CachingKeyValueStore(master, cache);
		doTestBackend(store);

		// set cache to offline - should still pass
		cache.setStatus(KeyValueStoreStatus.Offline);
		store.setStatus(KeyValueStoreStatus.Offline);
		doTestBackend(store);
	}

}
