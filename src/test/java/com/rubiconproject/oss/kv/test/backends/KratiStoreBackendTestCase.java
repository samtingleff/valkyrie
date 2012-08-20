package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.backends.KratiKeyValueStore;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class KratiStoreBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		KratiKeyValueStore store = new KratiKeyValueStore();
		store.setDir("tmp/krati");
		doTestBackend(store);
	}

}
