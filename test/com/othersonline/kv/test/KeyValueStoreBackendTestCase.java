package com.othersonline.kv.test;

import java.io.Serializable;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreStatus;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.ManagedKeyValueStore;
import com.othersonline.kv.backends.CachingKeyValueStore;
import com.othersonline.kv.backends.HashtableKeyValueStore;
import com.othersonline.kv.backends.MemcachedKeyValueStore;
import com.othersonline.kv.backends.OsCacheKeyValueStore;
import com.othersonline.kv.backends.ReplicatingKeyValueStore;
import com.othersonline.kv.backends.TokyoTyrantKeyValueStore;
import com.othersonline.kv.backends.WebDAVKeyValueStore;
import com.othersonline.kv.mgmt.JMXMbeanServerFactory;
import com.othersonline.kv.mgmt.KeyValueStoreMXBean;

import junit.framework.TestCase;

public class KeyValueStoreBackendTestCase extends TestCase {

	public void testHashtableBackend() throws Exception {
		KeyValueStore store = new HashtableKeyValueStore();
		doTestBackend(store);
	}

	public void testOsCacheBackend() throws Exception {
		KeyValueStore store = new OsCacheKeyValueStore();
		doTestBackend(store);
	}

	public void testMemcachedBackend() throws Exception {
		MemcachedKeyValueStore store = new MemcachedKeyValueStore();
		store.setUseBinaryProtocol(false);
		store.setHosts("localhost:11211");
		doTestBackend(store);

		// test counters
		String counterKey = "test.counter";
		store.incr(counterKey, 2, 5l, 1); // counter that expires in one second
		Object o = store.get(counterKey);
		assertNotNull(o);
		assertEquals(Long.parseLong((String) store.get(counterKey)), 5l);
		store.incr(counterKey, 10, 50l);
		assertEquals(Long.parseLong((String) store.get(counterKey)), 15l);
		store.decr(counterKey, 3, 55l);
		assertEquals(Long.parseLong((String) store.get(counterKey)), 12l);

		store.delete(counterKey);

		// test some jmx attributes
		ObjectName objectName = new ObjectName(store.getMXBeanObjectName());
		MBeanServer mbeanServer = JMXMbeanServerFactory.getMBeanServer();

		mbeanServer
				.invoke(objectName, "stop", new Object[] {}, new String[] {});
		assertEquals(store.getStatus(), KeyValueStoreStatus.Offline);
		mbeanServer.invoke(objectName, "start", new Object[] {},
				new String[] {});
		assertEquals(store.getStatus(), KeyValueStoreStatus.Online);
		String[] attributes = new String[] { "TotalObjectCount",
				"TotalByteCount", "TotalEvictions", "HitRatio" };
		for (String attribute : attributes) {
			Number n = (Number) mbeanServer.getAttribute(objectName, attribute);
			assertNotNull(n);
		}
	}

	public void testTokyoTyrantBackend() throws Exception {
		TokyoTyrantKeyValueStore store = new TokyoTyrantKeyValueStore();
		store.setHost("localhost");
		store.setPort(1978);
		doTestBackend(store);
	}

	public void testWebDavBackend() throws Exception {
		WebDAVKeyValueStore store = new WebDAVKeyValueStore();
		store.setBaseUrl("http://localhost/dav/testing/");
		doTestBackend(store);
	}

	public void testCachingStore() throws Exception {
		TokyoTyrantKeyValueStore master = new TokyoTyrantKeyValueStore();
		master.setHost("localhost");
		master.setPort(1978);
		master.start();

		MemcachedKeyValueStore cache = new MemcachedKeyValueStore();
		cache.setHosts("localhost:11211");
		cache.start();

		CachingKeyValueStore store = new CachingKeyValueStore(master, cache);
		doTestBackend(store);

		// set cache to offline - should still pass
		cache.setStatus(KeyValueStoreStatus.Offline);
		store.setStatus(KeyValueStoreStatus.Offline);
		doTestBackend(store);
	}

	public void testReplicatingStore() throws Exception {
		MemcachedKeyValueStore master = new MemcachedKeyValueStore();
		master.setHosts("localhost:11211");
		master.start();

		KeyValueStore replica = new HashtableKeyValueStore();
		replica.start();

		ReplicatingKeyValueStore store = new ReplicatingKeyValueStore(master);
		store.addReplica(replica);

		doTestBackend(store);

		// remove replica - should still pass test cases
		store.removeReplica(replica);
		store.setStatus(KeyValueStoreStatus.Offline);
		doTestBackend(store);

		// set status of replica to offline - should still pass
		store.addReplica(replica);
		replica.setStatus(KeyValueStoreStatus.Offline);
		store.setStatus(KeyValueStoreStatus.Offline);
		doTestBackend(store);
	}

	private void doTestBackend(KeyValueStore store) throws Exception {
		String key = "xyz";
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

		SampleV v = new SampleV(10, "hello world", 12);
		store.set(key, v);
		Thread.sleep(100l);
		SampleV v2 = (SampleV) store.get(key);
		assertNotNull(v2);
		assertEquals(v2.someRequiredInt, v.someRequiredInt);
		assertEquals(v2.someString, v.someString);
		assertEquals(v2.someOptionalDouble, v.someOptionalDouble);

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

	private static class SampleV implements Serializable {
		private static final long serialVersionUID = 7278340350600213753L;

		private int someRequiredInt;

		private String someString;

		private double someOptionalDouble;

		public SampleV() {
		}

		public SampleV(int someRequiredInt, String someString,
				double someOptionalDouble) {
			this.someRequiredInt = someRequiredInt;
			this.someString = someString;
			this.someOptionalDouble = someOptionalDouble;
		}

	}
}
