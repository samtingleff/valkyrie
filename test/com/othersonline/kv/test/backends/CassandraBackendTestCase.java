package com.othersonline.kv.test.backends;

import com.othersonline.kv.backends.CassandraKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class CassandraBackendTestCase extends KeyValueStoreBackendTestCase {

	@Override
	public void testBackend() throws Exception {
		CassandraKeyValueStore store = new CassandraKeyValueStore();
		doTestBackend(store);
	}
}
