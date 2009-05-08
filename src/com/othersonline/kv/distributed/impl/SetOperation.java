package com.othersonline.kv.distributed.impl;

import java.util.concurrent.Callable;

import com.othersonline.kv.distributed.AbstractOperation;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationResult;

public class SetOperation<V> extends AbstractOperation<V> implements
		Operation<V>, Callable<OperationResult<V>> {
	private static final long serialVersionUID = -6746808553560914494L;

	private V value;

	public SetOperation(String key, V value) {
		super(key);
		this.value = value;
	}

	public SetOperation<V> copy() {
		return new SetOperation<V>(this.key, this.value);
	}

	public OperationResult<V> call() throws Exception {
		try {
			store.set(key, value);
			OperationResult<V> result = new DefaultOperationResult<V>(this,
					node, null);
			return result;
		} finally {
		}
	}
}
