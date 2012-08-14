package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.backends.WebDAVKeyValueStore;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class WebDavBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		WebDAVKeyValueStore store = new WebDAVKeyValueStore();
		store.setBaseUrl("http://dev-db/dav/testing/");
		doTestBackend(store);
	}

}
