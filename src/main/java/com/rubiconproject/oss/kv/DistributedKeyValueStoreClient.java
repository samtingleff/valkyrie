package com.rubiconproject.oss.kv;

import java.io.IOException;
import java.util.List;

import com.rubiconproject.oss.kv.distributed.Configuration;
import com.rubiconproject.oss.kv.distributed.Context;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

public interface DistributedKeyValueStoreClient extends KeyValueStore {
	public Configuration getConfiguration();

	public <V> List<Context<V>> getContexts(String key,
			boolean considerNullAsSuccess, boolean enableSlidingWindow,
			long singleRequestTimeout, long operationTimeout)
			throws KeyValueStoreException, IOException;

	public <V> List<Context<V>> getContexts(String key, Transcoder transcoder,
			boolean considerNullAsSuccess, boolean enableSlidingWindow,
			long singleRequestTimeout, long operationTimeout)
			throws KeyValueStoreException, IOException;
}
