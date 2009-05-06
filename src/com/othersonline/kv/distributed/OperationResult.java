package com.othersonline.kv.distributed;

public interface OperationResult<V> {

	public Operation<V> getOperation();

	public Node getNode();

	public V getValue();
}
