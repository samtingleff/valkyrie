package com.othersonline.kv.distributed;

import java.io.IOException;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreUnavailable;

public interface ConnectionFactory {
	public KeyValueStore getStore(Node node) throws IOException,
			KeyValueStoreUnavailable;
}
