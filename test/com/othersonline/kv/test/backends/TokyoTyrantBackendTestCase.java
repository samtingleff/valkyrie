package com.othersonline.kv.test.backends;

import com.othersonline.kv.backends.TokyoTyrantKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class TokyoTyrantBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		TokyoTyrantKeyValueStore store = new TokyoTyrantKeyValueStore();
		store.setHost("dev-db");
		store.setPort(1978);
		store.setSocketTimeout(1000 * 5);
		doTestBackend(store);
		String[] keys = new String[] { "key1", "key2" };
		for (String key : keys) {
			store.set(key, "value");
		}
		Object[] fwmkeys = store.fwmkeys("key", 10);
		assertEquals(fwmkeys.length, 2);
		assertEquals(fwmkeys[0], keys[0]);
		assertEquals(fwmkeys[1], keys[1]);
		
		assertTrue(store.size() > 0);
		assertTrue(store.rnum() > 0);
		assertTrue(store.getStats() != null);

		for (String key : keys) {
			store.delete(key);
		}
		fwmkeys = store.fwmkeys("key", 10);
		assertEquals(fwmkeys.length, 0);

		assertTrue(store.optimize());
	}
}
