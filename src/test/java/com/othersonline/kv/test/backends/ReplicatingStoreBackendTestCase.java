package com.othersonline.kv.test.backends;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreStatus;
import com.othersonline.kv.backends.ConcurrentHashMapKeyValueStore;
import com.othersonline.kv.backends.MemcachedKeyValueStore;
import com.othersonline.kv.backends.ReplicatingKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

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
