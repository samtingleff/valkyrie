package com.othersonline.kv.distributed;

public interface OperationResult<V> {

	public Operation<V> getOperation();

	public V getValue();

	public OperationStatus getStatus();

	public long getDuration();

	public Throwable getError();
}
