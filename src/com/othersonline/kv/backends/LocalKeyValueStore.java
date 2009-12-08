package com.othersonline.kv.backends;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;

public interface LocalKeyValueStore extends KeyValueStore {

	public void sync() throws KeyValueStoreException;
}
