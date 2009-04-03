package com.othersonline.kv.test;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.backends.CachingKeyValueStore;
import com.othersonline.kv.backends.HashtableKeyValueStore;
import com.othersonline.kv.backends.MemcachedKeyValueStore;
import com.othersonline.kv.backends.OsCacheKeyValueStore;
import com.othersonline.kv.backends.ReplicatingKeyValueStore;
import com.othersonline.kv.backends.TokyoTyrantKeyValueStore;
import com.othersonline.kv.transcoder.LongTranscoder;
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

	public void testReplication() throws Exception {
		MemcachedKeyValueStore master = new MemcachedKeyValueStore();
		master.setHosts("localhost:11211");
		master.start();

		KeyValueStore replica1 = new OsCacheKeyValueStore();
		replica1.start();

		KeyValueStore replica2 = new HashtableKeyValueStore();
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
