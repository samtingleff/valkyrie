package com.rubiconproject.oss.kv.test.backends;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.rubiconproject.oss.kv.KeyValueStoreStatus;
import com.rubiconproject.oss.kv.backends.MemcachedKeyValueStore;
import com.rubiconproject.oss.kv.mgmt.JMXMbeanServerFactory;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class MemcachedBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		MemcachedKeyValueStore store = new MemcachedKeyValueStore();
		store.setUseBinaryProtocol(false);
		store.setHosts("localhost:11211");
		doTestBackend(store);

		// test transactions
		// doTestTransactions(store);

		// test counters
		String counterKey = "test.counter";
		store.incr(counterKey, 2, 5l, 1); // counter that expires in one
		// second
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

}
