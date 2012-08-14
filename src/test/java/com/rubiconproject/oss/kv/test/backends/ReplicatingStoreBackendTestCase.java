package com.rubiconproject.oss.kv.test.backends;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreStatus;
import com.rubiconproject.oss.kv.backends.ConcurrentHashMapKeyValueStore;
import com.rubiconproject.oss.kv.backends.MemcachedKeyValueStore;
import com.rubiconproject.oss.kv.backends.ReplicatingKeyValueStore;
import com.rubiconproject.oss.kv.test.KeyValueStoreBackendTestCase;

public class ReplicatingStoreBackendTestCase extends
		KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
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

}
