package com.othersonline.kv.test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import junit.framework.TestCase;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreStatus;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.ManagedKeyValueStore;
import com.othersonline.kv.backends.IterableKeyValueStore;
import com.othersonline.kv.backends.KeyValueStoreIterator;
import com.othersonline.kv.mgmt.JMXMbeanServerFactory;
import com.othersonline.kv.transcoder.ByteArrayTranscoder;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.tx.KeyValueStoreTransaction;
import com.othersonline.kv.tx.TransactionalKeyValueStore;

public abstract class KeyValueStoreBackendTestCase extends TestCase {

	public abstract void testBackend() throws Exception;

	protected void doTestBackend(KeyValueStore store) throws Exception {
		String key = "some/key";
		assertEquals(store.getStatus(), KeyValueStoreStatus.Offline);
		try {
			store.get(key);
			throw new Exception("Should not be available");
		} catch (KeyValueStoreUnavailable expected) {
		}

		store.start();
		assertEquals(store.getStatus(), KeyValueStoreStatus.Online);

		store.delete(key);
		assertNull(store.get(key));
		assertFalse(store.exists(key));

		store.set(key, "hello world");
		assertEquals(store.get(key), "hello world");

		SampleV v = new SampleV(10, "hello world", 12);
		store.set(key, v);
		Thread.sleep(100l);
		assertTrue(store.exists(key));
		SampleV v2 = (SampleV) store.get(key);
		assertNotNull(v2);
		assertEquals(v2.someRequiredInt, v.someRequiredInt);
		assertEquals(v2.someString, v.someString);
		assertEquals(v2.someOptionalDouble, v.someOptionalDouble);

		// test getBulk()
		Map<String, Object> map = store.getBulk("xyz123", "abcdefg",
				"sdfdsfer", "weruiwer");
		assertEquals(map.size(), 0);
		map = store.getBulk(key, "abcdefg");
		assertEquals(map.size(), 1);
		assertEquals(((SampleV) map.get(key)).someRequiredInt,
				v.someRequiredInt);
		map = store.getBulk(Arrays
				.asList(new String[] { "sxyzxv", "123", key }));
		assertEquals(map.size(), 1);
		assertEquals(((SampleV) map.get(key)).someRequiredInt,
				v.someRequiredInt);
		map = store.getBulk(Arrays
				.asList(new String[] { "12345", key, "sfdsdf" }),
				new ByteArrayTranscoder());
		assertEquals(map.size(), 1);

		// test iterator if applicable
		if (store instanceof IterableKeyValueStore)
			doTestIterator((IterableKeyValueStore) store);

		// set status to read only
		store.setStatus(KeyValueStoreStatus.ReadOnly);
		SampleV v3 = (SampleV) store.get(key);
		assertNotNull(v3);
		try {
			store.set(key, v3);
			throw new Exception("Status should be readonly");
		} catch (KeyValueStoreUnavailable e) {
		}
		store.setStatus(KeyValueStoreStatus.Online);

		// test start/stop
		store.stop();
		try {
			store.set(key, v3);
			throw new Exception("Status should be readonly");
		} catch (KeyValueStoreUnavailable e) {
		}
		store.start();

		store.delete(key);
		Thread.sleep(100l);
		assertNull(store.get(key));

		doTestJMX(store);
	}

	private void doTestIterator(IterableKeyValueStore store) throws Exception {
		int keyCount = 0;
		KeyValueStoreIterator storeIterator = store.iterkeys();
		Iterator<String> iter = storeIterator.iterator();
		while (iter.hasNext()) {
			String s = iter.next();
			++keyCount;
		}
		assertTrue(keyCount > 0);
		storeIterator.close();
	}

	private void doTestTransactions(TransactionalKeyValueStore store)
			throws Exception {
		String key = "test.tx.key";
		SampleV v = new SampleV(10, "hello world", 12.2d);
		Transcoder transcoder = new SerializableTranscoder();
		KeyValueStoreTransaction<SampleV> tx = store.txGet(key, transcoder);
		assertFalse(store.exists(key));
		assertNull(tx.getObject());

		tx.setObject(v);
		store.txSet(tx, key, transcoder);
		assertTrue(store.exists(key));

		// start two transactions at once
		tx = store.txGet(key, transcoder);
		KeyValueStoreTransaction<SampleV> tx2 = store.txGet(key, transcoder);
		// attempt to save the second
		tx2.getObject().someRequiredInt = 13;
		store.txSet(tx2, key);
	}

	private void doTestJMX(KeyValueStore store) throws Exception {
		if (!(store instanceof ManagedKeyValueStore))
			return;
		ManagedKeyValueStore managedStore = (ManagedKeyValueStore) store;
		ObjectName objectName = new ObjectName(managedStore
				.getMXBeanObjectName());
		MBeanServer mbeanServer = JMXMbeanServerFactory.getMBeanServer();

		ObjectInstance instance = mbeanServer.getObjectInstance(objectName);
		assertNotNull(instance);

		MBeanInfo info = mbeanServer.getMBeanInfo(objectName);
		assertNotNull(info);
		mbeanServer
				.invoke(objectName, "stop", new Object[] {}, new String[] {});
		assertEquals(store.getStatus(), KeyValueStoreStatus.Offline);
		mbeanServer.invoke(objectName, "start", new Object[] {},
				new String[] {});
		assertEquals(store.getStatus(), KeyValueStoreStatus.Online);
	}

	public static class SampleV implements Serializable, Comparable<SampleV> {
		private static final long serialVersionUID = 7278340350600213753L;

		public int someRequiredInt;

		public String someString;

		public double someOptionalDouble;

		public SampleV() {
		}

		public SampleV(int someRequiredInt, String someString,
				double someOptionalDouble) {
			this.someRequiredInt = someRequiredInt;
			this.someString = someString;
			this.someOptionalDouble = someOptionalDouble;
		}

		public boolean equals(SampleV v) {
			return someRequiredInt == v.someRequiredInt
					&& someString.equals(v.someString)
					&& someOptionalDouble == v.someOptionalDouble;
		}

		public int compareTo(SampleV v) {
			int result = new Integer(someRequiredInt).compareTo(new Integer(v.someRequiredInt));
			result = (result == 0) ? (someString.compareTo(v.someString)) : result;
			result = (result == 0) ? (new Double(someOptionalDouble).compareTo(new Double(v.someOptionalDouble))) : result;
			return result;
		}

	}
}
