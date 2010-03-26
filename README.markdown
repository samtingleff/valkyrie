# oo-kv-storage #

## Introduction ##

oo-kv-storage is a project to provide a consistent API and value-add for a variety of key-value storage backends.

It is meant to be easy to use and easily embedded into a Spring or other IoC containers.

Currently supported backends include:

- java.util.concurrent.ConcurrentHashMap
- [OsCache](http://www.opensymphony.com/oscache/ "OsCache")
- [memcached](http://www.danga.com/memcached/ "memcached") (and cousins [MemcacheQ](http://memcachedb.org/memcacheq/ "MemcacheQ") and [MemcacheDB](http://memcachedb.org/ "MemcacheDB"))
- [BDB JE](http://www.oracle.com/database/berkeley-db/je/ "BDB Java Edition")
- [BDB](http://www.oracle.com/database/berkeley-db/ "BDB native using JNI")
- [Tokyo Cabinet](http://1978th.net/tokyocabinet/ "Tokyo Tyrant")
- [Tokyo Tyrant](http://1978th.net/tokyotyrant/ "Tokyo Tyrant")
- [Cassandra](http://cassandra.apache.org/ "Cassandra")
- [Project Voldemort](http://project-voldemort.com/ "Project Voldemort")
- a simple file-system backed store
- WebDAV (tested against Apache mod_dav, nginx and lighttpd)
- [KosmosFS](http://kosmosfs.wiki.sourceforge.net/ "Kosmos File System")
- A custom [thrift](http://incubator.apache.org/thrift/ "Apache Thrift")-based proxy server that uses any of the above as backend

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

		// this will save to memcached then to tokyo tyrant
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

		// this will save to OsCache, then memcached, then Tokyo Tyrant
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

### Using Thrift Backend ###

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

### Replication and Load Balancing ###

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
   <td>ConcurrentHashMap</td>
   <td>147ms</td>
   <td>20,408</td>
  </tr>
  <tr>
   <td>OsCache</td>
   <td>167ms</td>
   <td>17,964</td>
  </tr>
  <tr>
   <td>File system</td>
   <td>965ms</td>
   <td>3,109</td>
  </tr>
  <tr>
   <td>MemcacheDB</td>
   <td>9,226ms</td>
   <td>325</td>
  </tr>
  <tr>
   <td>Memcached</td>
   <td>10,287ms</td>
   <td>292</td>
  </tr>
  <tr>
   <td>Tokyo Tyrant</td>
   <td>11,150ms</td>
   <td>269</td>
  </tr>
  <tr>
   <td>WebDAV (Apache 2.2)</td>
   <td>11,342ms</td>
   <td>265</td>
  </tr>
  <tr>
   <td>Thrift + FS</td>
   <td>12387ms</td>
   <td>242</td>
  </tr>
  <tr>
   <td>BDB JE</td>
   <td>13,983ms</td>
   <td>215</td>
  </tr>
  <tr>
   <td>Thrift + BDB JE</td>
   <td>17,127ms</td>
   <td>175</td>
  </tr>
 </tbody>
</table>

## Documentation ##

- [Javadoc API](http://samtingleff.github.com/oo-kv-storage/doc/api/)

