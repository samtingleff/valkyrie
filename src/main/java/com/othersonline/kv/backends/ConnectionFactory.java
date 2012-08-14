package com.othersonline.kv.backends;

import java.io.IOException;
import java.util.Map;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreUnavailable;

public interface ConnectionFactory {
	public KeyValueStore getStore(Map defaultProperties, String uri) throws IOException,
			KeyValueStoreUnavailable;
}
