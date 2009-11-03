package com.othersonline.kv.backends;

import com.othersonline.kv.KeyValueStoreException;

public interface LocalKeyValueStore {

	public void sync() throws KeyValueStoreException;

}
