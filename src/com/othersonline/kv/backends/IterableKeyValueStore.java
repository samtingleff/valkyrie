package com.othersonline.kv.backends;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;

public interface IterableKeyValueStore extends KeyValueStore {
	public KeyValueStoreIterator iterkeys() throws KeyValueStoreException;
}
