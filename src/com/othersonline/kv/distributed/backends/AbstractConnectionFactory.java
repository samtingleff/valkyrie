package com.othersonline.kv.distributed.backends;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Node;

public abstract class AbstractConnectionFactory implements ConnectionFactory {
	private Map<Integer, KeyValueStore> backends = new HashMap<Integer, KeyValueStore>();

	public KeyValueStore getStore(Node node) throws IOException,
			KeyValueStoreUnavailable {
		KeyValueStore store = backends.get(node.getId());
		if (store == null) {
			synchronized (this) {
				if (store == null) {
					// yes i realize this is an anti-pattern. if we create an
					// extra connection here
					// and there nobody is going to care.
					store = createStoreConnection(node);
					backends.put(node.getId(), store);
				}
			}
		}
		return store;
	}

	protected abstract KeyValueStore createStoreConnection(Node node)
			throws IOException, KeyValueStoreUnavailable;

}
