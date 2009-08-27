package com.othersonline.kv.test.backends;

import java.io.File;

import com.othersonline.kv.backends.TokyoCabinetKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class TokyoCabinetBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		testBTreeBackend();
		testHashBackend();
	}

	private void testBTreeBackend() throws Exception {
		File tempFile = File.createTempFile("test-bdb-dir", ".tcb");
		TokyoCabinetKeyValueStore store = new TokyoCabinetKeyValueStore(
				tempFile.getCanonicalPath());
		store.setBtree(true);
		doTestBackend(store);
	}

	private void testHashBackend() throws Exception {
		File tempFile = File.createTempFile("test-bdb-dir", ".tch");
		TokyoCabinetKeyValueStore store = new TokyoCabinetKeyValueStore(
				tempFile.getCanonicalPath());
		doTestBackend(store);
	}

}
