package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.ThreadPoolAsyncFlushQueue;
import com.rubiconproject.oss.kv.backends.AsyncFlushCachingKeyValueStore;
import com.rubiconproject.oss.kv.backends.ConcurrentHashMapKeyValueStore;
import com.rubiconproject.oss.kv.backends.MemcachedKeyValueStore;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class AsyncFlushCachingStoreBackendTestCase extends
		KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		MemcachedKeyValueStore master = new MemcachedKeyValueStore();
		master.setHosts("localhost:11211");
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

}
