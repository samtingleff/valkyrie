package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.backends.EhCacheKeyValueStore;
import com.rubiconproject.oss.kv.backends.ThriftKeyValueStore;
import com.rubiconproject.oss.kv.server.ThriftKeyValueServer;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class ThriftBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		// create backend
		EhCacheKeyValueStore backend = new EhCacheKeyValueStore();
		backend.start();

		// start server
		/*ThriftKeyValueServer server = new ThriftKeyValueServer();
		server.setBackend(backend);
		server.start();*/

		ThriftKeyValueStore store = new ThriftKeyValueStore();
		store.start();
		store.delete("foo");
//		boolean b = store.exists("foo");
//		System.out.println("b: " + b);
		//doTestBackend(store);
	}

}
