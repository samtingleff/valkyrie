package com.othersonline.kv;

import java.io.IOException;
import java.util.List;

import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.transcoder.Transcoder;

public interface DistributedKeyValueStoreClient extends KeyValueStore {
	public Configuration getConfiguration();

	public <V> List<Context<V>> getContexts(String key, boolean considerNullAsSuccess)
			throws KeyValueStoreException, IOException;

	public <V> List<Context<V>> getContexts(String key, Transcoder transcoder, boolean considerNullAsSuccess)
			throws KeyValueStoreException, IOException;
}
