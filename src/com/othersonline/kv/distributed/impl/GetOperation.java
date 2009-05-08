package com.othersonline.kv.distributed.impl;

import java.util.concurrent.Callable;

import com.othersonline.kv.distributed.AbstractOperation;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationResult;

public class GetOperation<V> extends AbstractOperation<V> implements
		Operation<V>, Callable<OperationResult<V>> {
	private static final long serialVersionUID = -3908847991063100534L;

	public GetOperation(String key) {
		super(key);
	}

	public GetOperation<V> copy() {
		return new GetOperation<V>(this.key);
	}

	public OperationResult<V> call() throws Exception {
		try {
			V v = (V) store.get(key);
			OperationResult<V> result = new DefaultOperationResult<V>(this,
					node, v);
			return result;
		} finally {
		}
	}

}
