# Valkyrie #

## Introduction ##

Valkyrie is a project to provide a consistent API and value-add for a variety of key-value storage backends.

It is meant to be easy to use and easily embedded into a Spring or other IoC containers.

Currently supported backends include:

- java.util.concurrent.ConcurrentHashMap
- [Ehcache](http://ehcache.org/ "Ehcache")
- [memcached](http://www.danga.com/memcached/ "memcached") (and cousins [MemcacheQ](http://memcachedb.org/memcacheq/ "MemcacheQ") and [MemcacheDB](http://memcachedb.org/ "MemcacheDB"))
- [Krati](http://sna-projects.com/krati/ "Ehcache")
- a simple file-system backed store
- WebDAV (tested against Apache mod_dav, nginx and lighttpd)
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

## Documentation ##

- [Javadoc API](http://samtingleff.github.com/oo-kv-storage/doc/api/)

## Maven ##

    <dependencies>
        <dependency>
            <groupId>com.rubicon.oss</groupId>
            <artifactId>valkyrie</artifactId>
        </dependency>
    </dependencies>

    <repositories>
        <repository>
            <id>samtingleff-maven-snapshot-repo</id>
            <url>https://github.com/samtingleff/maven-repo/raw/master/snapshots</url>
            <layout>default</layout>
            <releases>
                <enabled>false</enabled>
            </releases>
            <snapshots>
                <enabled>true</enabled>
                <updatePolicy>daily</updatePolicy>
            </snapshots>
        </repository>
    </repositories>
