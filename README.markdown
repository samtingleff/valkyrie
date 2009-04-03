# oo-kv-storage #

## Introduction ##

oo-kv-storage is a project to provide a consistent API and value-add for a variety of
key-value storage backends.

It is meant to be easy to use and easily embedded into a Spring or other IoC containers.

Currently supported backends include:

- java.util.Hashtable (probably useful just as an example)
- [OsCache](http://www.opensymphony.com/oscache/ "OsCache")
- [memcached](http://www.danga.com/memcached/ "memcached") (and cousins [MemcacheQ](http://memcachedb.org/memcacheq/ "MemcacheQ") and [MemcacheDB](http://memcachedb.org/ "MemcacheDB"))
- [Tokyo Tyrant](http://tokyocabinet.sourceforge.net/tyrantdoc/ "Tokyo Tyrant")
- a simple file-system backed store
- WebDAV (tested against Apache mod_dav, nginx and lighttpd)

## Examples ##

### Simple Caching ###

To create a caching store using memcached as cache and Tokyo Tyrant as
permanent storage:

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

### Three-level Caching ###

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

### Replication and Load Balancing ###

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

## Benchmarks ##

Using a [totally unscientific benchmark](http://github.com/samtingleff/oo-kv-storage/blob/5c9cea4c672dda6c7863f9b3a12b639e0c149b81/test/com/othersonline/kv/test/BenchmarkTestCase.java) with 10 concurrent threads and 100 repetitions per thread (time is average of five runs):

<table>
 <thead>
  <tr>
   <td>backend</td>
   <td>time</td>
   <td>ops/sec</td>
  </tr>
 </thead>
 <tbody>
  <tr>
   <td>OsCache</td>
   <td>329ms</td>
   <td>9102</td>
  </tr>
  <tr>
   <td>Hashtable</td>
   <td>468ms</td>
   <td>6405</td>
  </tr>
  <tr>
   <td>File system</td>
   <td>9143ms</td>
   <td>328</td>
  </tr>
  <tr>
   <td>MemcacheDB</td>
   <td>15571ms</td>
   <td>193</td>
  </tr>
  <tr>
   <td>Memcached</td>
   <td>16454ms</td>
   <td>182</td>
  </tr>
  <tr>
   <td>WebDAV (Apache 2.2)</td>
   <td>44734ms</td>
   <td>67</td>
  </tr>
 </tbody>
</table>

## Documentation ##

- [Javadoc API](http://samtingleff.github.com/oo-kv-storage/doc/api/)

