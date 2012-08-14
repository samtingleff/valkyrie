package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.backends.EhCacheKeyValueStore;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class EhCacheBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		KeyValueStore store = new EhCacheKeyValueStore();
		doTestBackend(store);
	}

}
