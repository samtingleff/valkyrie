package com.othersonline.kv.distributed;

import java.util.List;

public interface ExtractedContext<V> {
	public Context<V> getContext();

	public List<Operation<V>> getAdditionalOperations();

}
