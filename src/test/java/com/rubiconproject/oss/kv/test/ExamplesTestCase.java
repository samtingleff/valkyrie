package com.rubiconproject.oss.kv.test;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.backends.CachingKeyValueStore;
import com.rubiconproject.oss.kv.backends.ConcurrentHashMapKeyValueStore;
import com.rubiconproject.oss.kv.backends.EhCacheKeyValueStore;
import com.rubiconproject.oss.kv.backends.FileSystemKeyValueStore;
import com.rubiconproject.oss.kv.backends.MemcachedKeyValueStore;
import com.rubiconproject.oss.kv.backends.ReplicatingKeyValueStore;
import com.rubiconproject.oss.kv.backends.ThriftKeyValueStore;
import com.rubiconproject.oss.kv.gen.Constants;
import com.rubiconproject.oss.kv.server.ThriftKeyValueServer;
import com.rubiconproject.oss.kv.transcoder.LongTranscoder;
import com.rubiconproject.oss.kv.transcoder.StringTranscoder;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

import junit.framework.TestCase;

public class ExamplesTestCase extends TestCase {

	public void testSimpleCaching() throws Exception {
		ConcurrentHashMapKeyValueStore master = new ConcurrentHashMapKeyValueStore();
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
		ConcurrentHashMapKeyValueStore master = new ConcurrentHashMapKeyValueStore();
		master.start();

		MemcachedKeyValueStore memcached = new MemcachedKeyValueStore();
		memcached.setHosts("localhost:11211");
		memcached.start();

		EhCacheKeyValueStore ehcache = new EhCacheKeyValueStore();
		ehcache.start();

		CachingKeyValueStore secondCache = new CachingKeyValueStore(master,
				memcached);
		secondCache.start();

		CachingKeyValueStore firstCache = new CachingKeyValueStore(secondCache,
				ehcache);
		firstCache.start();

		// this will save to tokyo tyrant, then to memcached
		String key = "some.key";
		firstCache.set(key, new Integer(14));
		assertEquals(master.get(key), firstCache.get(key));
		assertEquals(master.get(key), ehcache.get(key));
		assertEquals(master.get(key), memcached.get(key));

		// stop ehcache and memcached
		ehcache.stop();
		memcached.stop();

		// this will bypass both caches
		Integer i = (Integer) firstCache.get(key);
		assertEquals(i, new Integer(14));

		// start back up
		memcached.start();
		ehcache.start();

		// this will delete globally
		firstCache.delete(key);

		// shutdown
		ehcache.stop();
	}

	public void testReplication() throws Exception {
		MemcachedKeyValueStore master = new MemcachedKeyValueStore();
		master.setHosts("localhost:11211");
		master.start();

		KeyValueStore replica1 = new EhCacheKeyValueStore();
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
		assertEquals(master.get(key, transcoder), value);

		// delete it
		replicatingStore.delete(key);
		Thread.sleep(100l);
		assertFalse(replicatingStore.exists(key));
		assertFalse(replica1.exists(key));
		assertFalse(replica2.exists(key));
		assertFalse(master.exists(key));
	}
}
