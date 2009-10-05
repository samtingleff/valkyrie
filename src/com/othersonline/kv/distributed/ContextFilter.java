package com.othersonline.kv.distributed;

import java.util.List;

public interface ContextFilter<V> {

	public ContextFilterResult<V> filter(List<Context<V>> contexts, int replicas)
			throws DistributedKeyValueStoreException;
}
