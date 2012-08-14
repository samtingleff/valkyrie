package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.backends.ConcurrentHashMapKeyValueStore;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class ConcurrentHashMapBackendTestCase extends
		KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		ConcurrentHashMapKeyValueStore store = new ConcurrentHashMapKeyValueStore();
		doTestBackend(store);
		store.setWriteSleepTime(100);
		store.setReadSleepTime(50);
		long start = System.currentTimeMillis();
		store.set("some.key", new Integer(10));
		assertTrue((System.currentTimeMillis() - start) >= 100);

		start = System.currentTimeMillis();
		store.get("some.key");
		assertTrue(((System.currentTimeMillis() - start) >= 50)
				&& ((System.currentTimeMillis() - start) <= 100));
	}

}
