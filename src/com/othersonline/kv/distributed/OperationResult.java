package com.othersonline.kv.distributed;

public interface OperationResult<V> {

	public Operation<V> getOperation();

	public Node getNode();

	public int getNodeRank();

	public V getValue();
}
