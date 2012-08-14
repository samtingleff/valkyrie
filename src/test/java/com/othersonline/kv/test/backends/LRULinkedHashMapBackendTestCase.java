package com.othersonline.kv.test.backends;

import com.othersonline.kv.backends.LRULinkedHashMapKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class LRULinkedHashMapBackendTestCase extends
		KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		LRULinkedHashMapKeyValueStore store = new LRULinkedHashMapKeyValueStore();
		doTestBackend(store);
	}

}
