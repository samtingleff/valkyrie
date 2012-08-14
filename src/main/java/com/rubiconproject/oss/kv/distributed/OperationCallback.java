package com.rubiconproject.oss.kv.distributed;

public interface OperationCallback<V> {

	public void completed(OperationResult<V> result);

}
