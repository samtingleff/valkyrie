package com.rubiconproject.oss.kv.backends;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;

public interface LocalKeyValueStore extends KeyValueStore {

	public void sync() throws KeyValueStoreException;
}
