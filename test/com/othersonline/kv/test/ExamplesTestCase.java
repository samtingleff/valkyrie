package com.othersonline.kv.test;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.backends.CachingKeyValueStore;
import com.othersonline.kv.backends.FileSystemKeyValueStore;
import com.othersonline.kv.backends.ConcurrentHashMapKeyValueStore;
import com.othersonline.kv.backends.MemcachedKeyValueStore;
import com.othersonline.kv.backends.OsCacheKeyValueStore;
import com.othersonline.kv.backends.ReplicatingKeyValueStore;
import com.othersonline.kv.backends.ThriftKeyValueStore;
import com.othersonline.kv.backends.TokyoTyrantKeyValueStore;
import com.othersonline.kv.backends.VoldemortKeyValueStore;
import com.othersonline.kv.gen.Constants;
import com.othersonline.kv.server.ThriftKeyValueServer;
import com.othersonline.kv.transcoder.LongTranscoder;
import com.othersonline.kv.transcoder.StringTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

import junit.framework.TestCase;

public class ExamplesTestCase extends TestCase {

	public void testSimpleCaching() throws Exception {
		TokyoTyrantKeyValueStore master = new TokyoTyrantKeyValueStore();
		master.setHost("localhost");
		master.setPort(1978);
		master.start();

		MemcachedKeyValueStore cache = new MemcachedKeyValueStore();
		cache.setHosts("localhost:11211");
		cache.start();

		CachingKeyValueStore store = new CachingKeyValueStore();
		store.setMaster(master);
		store.setCache(cache);
		store.start();

		// this will save to tokyo tyrant, then to memcached
		String key = "some.key";
		store.set(key, new Integer(14));
		assertEquals(master.get(key), cache.get(key));
		// this will come from cache
		Integer i = (Integer) store.get(key);
		// this will delete on both
		store.delete(key);
	}

	public void testThreeLevelCaching() throws Exception {
		TokyoTyrantKeyValueStore master = new TokyoTyrantKeyValueStore();
		master.setHost("localhost");
		master.setPort(1978);
		master.start();

		MemcachedKeyValueStore memcached = new MemcachedKeyValueStore();
		memcached.setHosts("localhost:11211");
		memcached.start();

		OsCacheKeyValueStore oscache = new OsCacheKeyValueStore();
		oscache.start();

		CachingKeyValueStore secondCache = new CachingKeyValueStore(master,
				memcached);
		secondCache.start();

		CachingKeyValueStore firstCache = new CachingKeyValueStore(secondCache,
				oscache);
		firstCache.start();

		// this will save to tokyo tyrant, then to memcached
		String key = "some.key";
		firstCache.set(key, new Integer(14));
		assertEquals(master.get(key), firstCache.get(key));
		assertEquals(master.get(key), oscache.get(key));
		assertEquals(master.get(key), memcached.get(key));

		// stop oscache and memcached
		oscache.stop();
		memcached.stop();

		// this will bypass both caches
		Integer i = (Integer) firstCache.get(key);
		assertEquals(i, new Integer(14));

		// start back up
		memcached.start();
		oscache.start();

		// this will delete globally
		firstCache.delete(key);
	}

	public void testVoldemortClient() throws Exception {
		VoldemortKeyValueStore store = new VoldemortKeyValueStore();
		store.start();
		
		Transcoder transcoder = new StringTranscoder();
		String key = "abc";
		String value1 = "1234567890";
		String value2 = "0987654321";
		store.set(key, value1, transcoder);
		store.set(key, value2, transcoder);
	}
	public void testThriftServer() throws Exception {
		// Presumably (1) and (2) occurr on a different host from (3)
		// (1) create backing store for thrift service
		FileSystemKeyValueStore backend = new FileSystemKeyValueStore("tmp/fs");
		backend.start();

		// (2) start thrift service
		ThriftKeyValueServer server = new ThriftKeyValueServer();
		server.setBackend(backend);
		server.start();

		// (3) create client
		ThriftKeyValueStore client = new ThriftKeyValueStore("localhost",
				Constants.DEFAULT_PORT);
		client.start();

		String key = "some.key";
		client.set(key, new Integer(14));
		assertTrue(client.exists(key));
		assertEquals(client.get(key), new Integer(14));
		client.delete(key);
	}

	public void testReplication() throws Exception {
		MemcachedKeyValueStore master = new MemcachedKeyValueStore();
		master.setHosts("localhost:11211");
		master.start();

		KeyValueStore replica1 = new OsCacheKeyValueStore();
		replica1.start();

		KeyValueStore replica2 = new ConcurrentHashMapKeyValueStore();
		replica2.start();

		ReplicatingKeyValueStore replicatingStore = new ReplicatingKeyValueStore();
		replicatingStore.setMaster(master);
		replicatingStore.addReplica(replica1);
		replicatingStore.addReplica(replica2);
		replicatingStore.start();

		String key = "test.key";
		Long value = 12312312323l;
		Transcoder transcoder = new LongTranscoder();
		replicatingStore.set(key, value, transcoder);

		// replication occurs in a background thread - wait
		Thread.sleep(100l);
		// should be equal
		assertEquals(replicatingStore.get(key), value);
		assertEquals(replica1.get(key), value);
		assertEquals(replica2.get(key), value);
		assertEquals(master.get(key), value);

		// delete it
		replicatingStore.delete(key);
		Thread.sleep(100l);
		assertFalse(replicatingStore.exists(key));
		assertFalse(replica1.exists(key));
		assertFalse(replica2.exists(key));
		assertFalse(master.exists(key));
	}
}
