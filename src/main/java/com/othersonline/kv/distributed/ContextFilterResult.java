package com.othersonline.kv.distributed;

import java.util.List;

public interface ContextFilterResult<V> {

	public List<Operation<V>> getAdditionalOperations();

	public Context<V> getContext();
}
