package com.othersonline.kv;

import java.io.IOException;
import java.util.List;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.transcoder.Transcoder;

public interface DistributedKeyValueStoreClient extends KeyValueStore {
	public <V> List<Context<V>> getContexts(String key)
			throws KeyValueStoreException, IOException;

	public <V> List<Context<V>> getContexts(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException;
}
