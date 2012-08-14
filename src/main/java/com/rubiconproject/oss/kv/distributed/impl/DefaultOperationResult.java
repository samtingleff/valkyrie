package com.rubiconproject.oss.kv.distributed.impl;

import com.rubiconproject.oss.kv.distributed.Operation;
import com.rubiconproject.oss.kv.distributed.OperationResult;
import com.rubiconproject.oss.kv.distributed.OperationStatus;

public class DefaultOperationResult<V> implements OperationResult<V> {
	private Operation<V> operation;

	private V value;

	private OperationStatus status;

	private long duration;

	private Throwable error;

	public DefaultOperationResult(Operation<V> operation, V value,
			OperationStatus status, long duration, Throwable error) {
		this.operation = operation;
		this.value = value;
		this.status = status;
		this.duration = duration;
		this.error = error;
	}

	public V getValue() {
		return value;
	}

	public Operation<V> getOperation() {
		return operation;
	}

	public OperationStatus getStatus() {
		return status;
	}

	public long getDuration() {
		return duration;
	}

	public Throwable getError() {
		return error;
	}

}
