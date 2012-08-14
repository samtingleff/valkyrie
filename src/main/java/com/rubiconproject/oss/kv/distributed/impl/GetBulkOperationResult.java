package com.rubiconproject.oss.kv.distributed.impl;

import java.util.Map;

import com.rubiconproject.oss.kv.distributed.BulkOperation;
import com.rubiconproject.oss.kv.distributed.BulkOperationResult;
import com.rubiconproject.oss.kv.distributed.OperationStatus;

public class GetBulkOperationResult<V> extends DefaultOperationResult<V> implements BulkOperationResult<V> {

	private Map<String, V> values;

	public GetBulkOperationResult(GetBulkOperation<V> operation,
			Map<String, V> values, OperationStatus status, long duration,
			Throwable error) {
		super(operation, null, status, duration, error);
		this.values = values;
	}

	public BulkOperation<V> getBulkOperation() {
		return (BulkOperation<V>)getOperation();
	}
	
	public Map<String, V> getValues() {
		return values;
	}
}
