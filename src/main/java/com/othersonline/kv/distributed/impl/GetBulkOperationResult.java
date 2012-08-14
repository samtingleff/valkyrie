package com.othersonline.kv.distributed.impl;

import java.util.Map;

import com.othersonline.kv.distributed.BulkOperation;
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

	public BulkOperation<V> getBulkOperation() {
		return (BulkOperation<V>)getOperation();
	}
	
	public Map<String, V> getValues() {
		return values;
	}
}
