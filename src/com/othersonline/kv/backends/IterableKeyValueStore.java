package com.othersonline.kv.backends;

import com.othersonline.kv.KeyValueStoreException;

public interface IterableKeyValueStore {
	public KeyValueStoreIterator iterkeys() throws KeyValueStoreException;
}
