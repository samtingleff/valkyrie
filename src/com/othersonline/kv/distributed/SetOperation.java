package com.othersonline.kv.distributed;

import java.util.concurrent.Callable;

public class SetOperation<V> extends AbstractOperation<V> implements
		Operation<V>, Callable<OperationResult<V>> {
	private static final long serialVersionUID = -6746808553560914494L;

	private V value;

	public SetOperation(OperationCallback<V> callback, Node node, String key,
			V value) {
		super(callback, node, key);
		this.value = value;
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
