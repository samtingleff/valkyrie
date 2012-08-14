package com.othersonline.kv.test.backends;

import com.othersonline.kv.backends.OsCacheKeyValueStore;
import com.othersonline.kv.backends.ThriftKeyValueStore;
import com.othersonline.kv.server.ThriftKeyValueServer;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class ThriftBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		// create backend
		OsCacheKeyValueStore backend = new OsCacheKeyValueStore();
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
