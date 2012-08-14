package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.backends.LRULinkedHashMapKeyValueStore;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class LRULinkedHashMapBackendTestCase extends
		KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		LRULinkedHashMapKeyValueStore store = new LRULinkedHashMapKeyValueStore();
		doTestBackend(store);
	}

}
