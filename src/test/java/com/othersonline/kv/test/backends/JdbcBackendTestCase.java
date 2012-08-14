package com.othersonline.kv.test.backends;

import com.othersonline.kv.backends.JdbcKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;
import com.othersonline.kv.test.sql.SampleValueObject;
import com.othersonline.kv.test.sql.SampleValueObjectDAO;

public class JdbcBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		JdbcKeyValueStore store = new JdbcKeyValueStore();
		store.setUrl("jdbc:mysql://dev-db/kv");
		store.setUsername("haymitch");
		store.setPassword("haymitch");
		store.setTable("test_table");
		store.setKeyField("id");
		store.setValueField("value");
		doTestBackend(store);
		String[] keys = new String[] { "key1", "key2" };
		for (String key : keys) {
			store.set(key, "value");
		}

		assertTrue(store.size() > 0);

		for (String key : keys) {
			store.delete(key);
		}
	}

	public void testSampleValueObject() throws Exception {
		JdbcKeyValueStore store = new JdbcKeyValueStore();
		store.setUrl("jdbc:mysql://dev-db/kv");
		store.setUsername("haymitch");
		store.setPassword("haymitch");
		store.setDAOClass(SampleValueObjectDAO.class.getName());
		store.start();

		String key = "key1";
		SampleValueObject svo = new SampleValueObject(key, 12, 90, "value1");
		store.set(svo.getK(), svo);
		assertTrue(store.exists(key));

		SampleValueObject svo2 = (SampleValueObject) store.get(key);
		assertEquals(key, svo2.getK());
		assertEquals(svo.getS(), svo2.getS());
		assertEquals(svo.getX(), svo2.getX());
		assertEquals(svo.getY(), svo2.getY());
		store.delete(key);
		assertFalse(store.exists(key));

	}
}
