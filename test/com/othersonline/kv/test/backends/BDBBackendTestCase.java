package com.othersonline.kv.test.backends;

import java.io.File;

import com.othersonline.kv.backends.BDBJEKeyValueStore;
import com.othersonline.kv.backends.BDBKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class BDBBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		File tempFile = File.createTempFile("test-bdb-jni-dir", ".tmp");
		tempFile.delete();
		tempFile.mkdir();
		BDBKeyValueStore store = new BDBKeyValueStore(tempFile.getAbsolutePath());
		store.setAllowCreate(true);
		doTestBackend(store);
	}

}
