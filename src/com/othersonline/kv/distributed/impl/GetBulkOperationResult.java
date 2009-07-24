package com.othersonline.kv.distributed.impl;

import java.util.Map;

import com.othersonline.kv.distributed.OperationStatus;
import com.othersonline.kv.distributed.BulkOperationResult;

public class GetBulkOperationResult<V> extends DefaultOperationResult<V> implements BulkOperationResult<V> {

	private Map<String, V> values;

	public GetBulkOperationResult(GetBulkOperation<V> operation,
			Map<String, V> values, OperationStatus status, long duration,
			Throwable error) {
		super(operation, null, status, duration, error);
		this.values = values;
	}

	public GetBulkOperation<V> getBulkOperation() {
		return (GetBulkOperation<V>)getOperation();
	}
	
	public Map<String, V> getValues() {
		return values;
	}
}
