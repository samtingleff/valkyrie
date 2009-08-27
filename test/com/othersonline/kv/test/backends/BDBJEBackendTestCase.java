package com.othersonline.kv.test.backends;

import java.io.File;

import com.othersonline.kv.backends.BDBJEKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class BDBJEBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		File tempFile = File.createTempFile("test-bdb-dir", ".tmp");
		tempFile.delete();
		tempFile.mkdir();
		BDBJEKeyValueStore store = new BDBJEKeyValueStore(tempFile);
		doTestBackend(store);
	}

}
