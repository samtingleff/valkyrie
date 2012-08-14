package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.backends.FileSystemKeyValueStore;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;
import com.rubiconproject.oss.kv.transcoder.StringTranscoder;

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
