package com.othersonline.kv.test.backends;

import com.othersonline.kv.backends.KosmosfsKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class KosmosfsBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		KosmosfsKeyValueStore store = new KosmosfsKeyValueStore();
		store.setMetaServerHost("localhost"); store.setMetaServerPort(20000);
		doTestBackend(store);
	}

}
