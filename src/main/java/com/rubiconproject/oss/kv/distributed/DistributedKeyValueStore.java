package com.rubiconproject.oss.kv.distributed;

import java.util.List;

import com.rubiconproject.oss.kv.KeyValueStoreException;

public interface DistributedKeyValueStore {

	public Configuration getConfiguration();

	public List<Context<byte[]>> getContexts(String key,
			boolean considerNullAsSuccess, boolean enableSlidingWindow,
			long singleRequestTimeout, long operationTimeout)
			throws KeyValueStoreException;

	public Context<byte[]> get(String key) throws KeyValueStoreException;

	public Context<byte[]> get(String key, ContextFilter<byte[]> contextFilter)
			throws KeyValueStoreException;

	public void set(String key, byte[] bytes) throws KeyValueStoreException;

	public void set(String key, Context<byte[]> context)
			throws KeyValueStoreException;

	public void delete(String key) throws KeyValueStoreException;
}
