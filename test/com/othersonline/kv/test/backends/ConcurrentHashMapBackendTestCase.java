package com.othersonline.kv.test.backends;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.backends.ConcurrentHashMapKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class ConcurrentHashMapBackendTestCase extends
		KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		KeyValueStore store = new ConcurrentHashMapKeyValueStore();
		doTestBackend(store);
	}

}
