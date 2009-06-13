package com.othersonline.kv.distributed;

public interface OperationCallback<V> {

	public void success(Node node, OperationResult<V> result);

	public void error(Node node, OperationResult<V> result, Exception e);
}
