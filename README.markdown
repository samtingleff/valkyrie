# oo-kv #

## Introduction ##

oo-kv is a project to provide a consistent API and value-add for a variety of
key-value storage backends.

It is meant to be easy to use and easily embedded into a Spring or other IoC containers.

Currently supported backends include:
 *   java.util.Hashtable (probably useful just as an example)
 *   [OsCache](http://www.opensymphony.com/oscache/ "OsCache")
 *   [memcached](http://www.danga.com/memcached/ "memcached") (and cousins [MemcacheQ](http://memcachedb.org/memcacheq/ "MemcacheQ") and [MemcacheDB](http://memcachedb.org/ "MemcacheDB"))
 *   [Tokyo Tyrant](http://tokyocabinet.sourceforge.net/tyrantdoc/ "Tokyo Tyrant")
 *   WebDAV (tested against Apache mod_dav, nginx and lighttpd)

## Examples ##

### Simple Caching ###

To create a caching store using memcached as cache and Tokyo Tyrant as
permanent storage:

		TokyoTyrantKeyValueStore master = new TokyoTyrantKeyValueStore();
		master.setHost("localhost");
		master.setPort(1978);
		master.init();

		MemcachedKeyValueStore cache = new MemcachedKeyValueStore();
		cache.setHosts("localhost:11211");
		cache.init();
		
		CachingKeyValueStore store = new CachingKeyValueStore();
		store.setMaster(master);
		store.setCache(cache);
		store.init();
		// this will save to tokyo tyrant, then to memcached
		store.set("some.key", new Integer(14));
		// this will come from cache
		Integer i = (Integer) store.get("some.key");
		// this will delete on both
		store.delete("some.key");

### Replication and Load Balancing ###

