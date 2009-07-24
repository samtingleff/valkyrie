package com.othersonline.kv.distributed.impl;

import java.util.Map;

import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationStatus;

public class GetBulkOperationResult<V> extends DefaultOperationResult<V> {

	private Map<String, V> results;

	public GetBulkOperationResult(Operation<V> operation,
			Map<String, V> results, OperationStatus status, long duration,
			Throwable error) {
		super(operation, null, status, duration, error);
		this.results = results;
	}

	public Map<String, V> getResults() {
		return results;
	}
}
