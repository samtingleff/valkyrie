package com.rubiconproject.oss.kv.distributed;

import java.util.List;

public interface ContextFilter<V> {

	public ContextFilterResult<V> filter(List<Context<V>> contexts)
			throws DistributedKeyValueStoreException;
}
