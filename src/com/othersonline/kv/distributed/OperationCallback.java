package com.othersonline.kv.distributed;

public interface OperationCallback<V> {

	public void success(OperationResult<V> result);

	public void error(OperationResult<V> result, Exception e);
}
