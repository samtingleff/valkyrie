package com.othersonline.kv.test.backends;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.backends.OsCacheKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class OsCacheBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		KeyValueStore store = new OsCacheKeyValueStore();
		doTestBackend(store);
	}

}
