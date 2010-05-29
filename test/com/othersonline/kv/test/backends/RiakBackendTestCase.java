package com.othersonline.kv.test.backends;

import com.othersonline.kv.backends.RiakKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class RiakBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		RiakKeyValueStore store = new RiakKeyValueStore();
		store.setBucket("test");
		store.setBaseUrl("http://dev-db:8098/riak/");
		doTestBackend(store);
	}

}
