package com.rubiconproject.oss.kv.backends;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;

public interface IterableKeyValueStore extends KeyValueStore {
	public KeyValueStoreIterator iterkeys() throws KeyValueStoreException;
}
