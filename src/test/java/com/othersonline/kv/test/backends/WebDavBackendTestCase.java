package com.othersonline.kv.test.backends;

import com.othersonline.kv.backends.WebDAVKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class WebDavBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		WebDAVKeyValueStore store = new WebDAVKeyValueStore();
		store.setBaseUrl("http://dev-db/dav/testing/");
		doTestBackend(store);
	}

}
