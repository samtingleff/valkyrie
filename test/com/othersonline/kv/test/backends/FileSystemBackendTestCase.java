package com.othersonline.kv.test.backends;

import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.backends.FileSystemKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;
import com.othersonline.kv.transcoder.StringTranscoder;

public class FileSystemBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		FileSystemKeyValueStore store = new FileSystemKeyValueStore();
		store.setRoot("tmp/fs");
		store.setCleanEmptyDirectories(true);
		doTestBackend(store);
		try {
			String s = (String) store.get("../../../../etc/passwd",
					new StringTranscoder());
			throw new Exception("should not be here");
		} catch (KeyValueStoreException e) {
		}
		try {
			store.set("../../some/wacky/path.txt", "my object");
			throw new Exception("should not be here");
		} catch (KeyValueStoreException e) {
		}
	}

}
