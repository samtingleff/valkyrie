package com.othersonline.kv.test.backends;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.backends.EhCacheKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class EhCacheBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		KeyValueStore store = new EhCacheKeyValueStore();
		doTestBackend(store);
	}

}
