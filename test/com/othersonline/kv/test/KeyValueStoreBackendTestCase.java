package com.othersonline.kv.test;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.KeyValueStoreStatus;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.ManagedKeyValueStore;
import com.othersonline.kv.ThreadPoolAsyncFlushQueue;
import com.othersonline.kv.backends.AsyncFlushCachingKeyValueStore;
import com.othersonline.kv.backends.BDBJEKeyValueStore;
import com.othersonline.kv.backends.CachingKeyValueStore;
import com.othersonline.kv.backends.FileSystemKeyValueStore;
import com.othersonline.kv.backends.ConcurrentHashMapKeyValueStore;
import com.othersonline.kv.backends.KosmosfsKeyValueStore;
import com.othersonline.kv.backends.MemcachedKeyValueStore;
import com.othersonline.kv.backends.OsCacheKeyValueStore;
import com.othersonline.kv.backends.RateLimitingKeyValueStore;
import com.othersonline.kv.backends.ReplicatingKeyValueStore;
import com.othersonline.kv.backends.ThriftKeyValueStore;
import com.othersonline.kv.backends.TokyoTyrantKeyValueStore;
import com.othersonline.kv.backends.VoldemortKeyValueStore;
import com.othersonline.kv.backends.WebDAVKeyValueStore;
import com.othersonline.kv.distributed.impl.DistributedKeyValueStoreClientImpl;
import com.othersonline.kv.distributed.impl.PropertiesConfigurator;
import com.othersonline.kv.mgmt.JMXMbeanServerFactory;
import com.othersonline.kv.server.ThriftKeyValueServer;
import com.othersonline.kv.transcoder.ByteArrayTranscoder;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.StringTranscoder;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.tx.KeyValueStoreTransaction;
import com.othersonline.kv.tx.TransactionalKeyValueStore;
import com.othersonline.kv.util.MemcachedRateLimiter;
import com.othersonline.kv.util.RateLimiter;
import com.othersonline.kv.util.SimpleRateLimiter;

import junit.framework.TestCase;

public class KeyValueStoreBackendTestCase extends TestCase {

	public void testHashtableBackend() throws Exception {
		KeyValueStore store = new ConcurrentHashMapKeyValueStore();
		doTestBackend(store);
	}

	public void testOsCacheBackend() throws Exception {
		KeyValueStore store = new OsCacheKeyValueStore();
		doTestBackend(store);
	}

	public void testMemcachedBackend() throws Exception {
		MemcachedKeyValueStore store = new MemcachedKeyValueStore();
		store.setUseBinaryProtocol(false);
		store.setHosts("dev-db:11211");
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

	public void testTokyoTyrantBackend() throws Exception {
		TokyoTyrantKeyValueStore store = new TokyoTyrantKeyValueStore();
		store.setHost("dev-db");
		store.setPort(1978);
		doTestBackend(store);
		String[] keys = new String[] { "key1", "key2" };
		for (String key : keys) {
			store.set(key, "value");
		}
		Object[] fwmkeys = store.fwmkeys("key", 10);
		assertEquals(fwmkeys.length, 2);
		assertEquals(fwmkeys[0], keys[0]);
		assertEquals(fwmkeys[1], keys[1]);
		for (String key : keys) {
			store.delete(key);
		}
		fwmkeys = store.fwmkeys("key", 10);
		assertEquals(fwmkeys.length, 0);
	}

	public void testFileSystemBackend() throws Exception {
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

	public void testWebDavBackend() throws Exception {
		WebDAVKeyValueStore store = new WebDAVKeyValueStore();
		store.setBaseUrl("http://dev-db/dav/testing/");
		doTestBackend(store);
	}

	public void testBDBJEBackend() throws Exception {
		File tempFile = File.createTempFile("test-bdb-dir", ".tmp");
		tempFile.delete();
		tempFile.mkdir();
		BDBJEKeyValueStore store = new BDBJEKeyValueStore(tempFile);
		doTestBackend(store);
	}

	public void testVoldemortBackend() throws Exception {
		VoldemortKeyValueStore store = new VoldemortKeyValueStore();
		store.setBootstrapUrl("tcp://dev-db:6666");
		doTestBackend(store);
	}

	public void testKosmosfsBackend() throws Exception {
		/*KosmosfsKeyValueStore store = new KosmosfsKeyValueStore();
		store.setMetaServerHost("localhost"); store.setMetaServerPort(20000);
		doTestBackend(store);*/
	}

	public void testHaymitchBackend() throws Exception {
		PropertiesConfigurator configurator = new PropertiesConfigurator();
		configurator
				.load(getClass()
						.getResourceAsStream(
								"/com/othersonline/kv/test/resources/haymitch-test.properties"));
		DistributedKeyValueStoreClientImpl store = new DistributedKeyValueStoreClientImpl();
		store.setConfigurator(configurator);
		doTestBackend(store);
	}

	public void testThriftBackend() throws Exception {
		// create backend
		OsCacheKeyValueStore backend = new OsCacheKeyValueStore();
		backend.start();

		// start server
		ThriftKeyValueServer server = new ThriftKeyValueServer();
		server.setBackend(backend);
		server.start();

		ThriftKeyValueStore store = new ThriftKeyValueStore();
		doTestBackend(store);
	}

	public void testCachingStore() throws Exception {
		TokyoTyrantKeyValueStore master = new TokyoTyrantKeyValueStore();
		master.setHost("dev-db");
		master.setPort(1978);
		master.start();

		MemcachedKeyValueStore cache = new MemcachedKeyValueStore();
		cache.setHosts("dev-db:11211");
		cache.start();

		CachingKeyValueStore store = new CachingKeyValueStore(master, cache);
		doTestBackend(store);

		// set cache to offline - should still pass
		cache.setStatus(KeyValueStoreStatus.Offline);
		store.setStatus(KeyValueStoreStatus.Offline);
		doTestBackend(store);
	}

	public void testAsyncFlushCachingStore() throws Exception {
		MemcachedKeyValueStore master = new MemcachedKeyValueStore();
		master.setHosts("dev-db:11211");
		master.start();

		KeyValueStore cache = new ConcurrentHashMapKeyValueStore();
		cache.start();

		AsyncFlushCachingKeyValueStore store = new AsyncFlushCachingKeyValueStore(
				master, cache, new ThreadPoolAsyncFlushQueue(master, 1));
		store.start();

		String key = "xyz";
		SampleV v = new SampleV(10, "hello world", 12);
		store.set(key, v);
		Thread.sleep(100l);

		// should be flushed to master
		SampleV v2 = (SampleV) master.get(key);
		assertNotNull(v2);
		assertEquals(v2.someRequiredInt, v.someRequiredInt);
		assertEquals(v2.someString, v.someString);
		assertEquals(v2.someOptionalDouble, v.someOptionalDouble);
		store.delete(key);
		Thread.sleep(100l);
	}

	public void testReplicatingStore() throws Exception {
		MemcachedKeyValueStore master = new MemcachedKeyValueStore();
		master.setHosts("dev-db:11211");
		master.start();

		KeyValueStore replica = new ConcurrentHashMapKeyValueStore();
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

	public void testRateLimitingKeyValueStore() throws Exception {
		MemcachedKeyValueStore mcc = new MemcachedKeyValueStore();
		mcc.setHosts("dev-db:11211");
		mcc.start();

		RateLimitingKeyValueStore store = new RateLimitingKeyValueStore();
		store.setMaster(mcc);
		// test behavior w/ no limits
		doTestBackend(store);

		// add a write limit of one per 100ms
		RateLimiter limiter = new SimpleRateLimiter(TimeUnit.MILLISECONDS, 100,
				1);
		store.setWriteRateLimiter(limiter);
		store.set("test.key", "test.value");
		try {
			store.set("test.key2", "test.value2");
			fail("Rate limit exceeded. Should have failed!");
		} catch (KeyValueStoreUnavailable e) {
			assertEquals(limiter.getCounter(), 1);
		}
		// sleep for 100ms - should succeed
		Thread.sleep(100l);
		store.set("test.key2", "test.value2");
		assertEquals(limiter.getCounter(), 1);

		// test a memcached rate limiter (2 per sec.)
		limiter = new MemcachedRateLimiter(mcc);
		limiter.setLimit(TimeUnit.SECONDS, 1, 2);
		store.setWriteRateLimiter(limiter);
		store.set("test.key", "test.value");
		store.set("test.key", "test.value.2");
		try {
			store.set("test.key2", "test.value3");
			fail("Rate limit exceeded. Should have failed!");
		} catch (KeyValueStoreUnavailable e) {
			assertEquals(limiter.getCounter(), 2);
		}
		// sleep for 1000ms - should succeed
		Thread.sleep(1000l);
		store.set("test.key2", "test.value2");
		assertEquals(limiter.getCounter(), 1);
	}

	private void doTestBackend(KeyValueStore store) throws Exception {
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
