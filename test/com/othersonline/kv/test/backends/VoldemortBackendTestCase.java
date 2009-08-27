package com.othersonline.kv.test.backends;

import com.othersonline.kv.backends.VoldemortKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class VoldemortBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		VoldemortKeyValueStore store = new VoldemortKeyValueStore();
		store.setBootstrapUrl("tcp://dev-db:6666");
		doTestBackend(store);
	}

}
