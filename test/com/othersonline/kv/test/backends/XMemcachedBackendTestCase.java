package com.othersonline.kv.test.backends;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.othersonline.kv.KeyValueStoreStatus;
import com.othersonline.kv.backends.XMemcachedKeyValueStore;
import com.othersonline.kv.mgmt.JMXMbeanServerFactory;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class XMemcachedBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		XMemcachedKeyValueStore store = new XMemcachedKeyValueStore();
		store.setUseBinaryProtocol(false);
		store.setHosts("dev-db:11211");
		doTestBackend(store);

		// test transactions
		// doTestTransactions(store);

		// test counters
		String counterKey = "test.counter";
		store.incr(counterKey, 2, 5l);
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
