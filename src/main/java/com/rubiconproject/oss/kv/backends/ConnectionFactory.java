package com.rubiconproject.oss.kv.backends;

import java.io.IOException;
import java.util.Map;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreUnavailable;

public interface ConnectionFactory {
	public KeyValueStore getStore(Map defaultProperties, String uri) throws IOException,
			KeyValueStoreUnavailable;
}
