package com.othersonline.kv.distributed.impl;

import com.othersonline.kv.distributed.AbstractOperation;
import com.othersonline.kv.distributed.OperationResult;

public class DeleteOperation<V> extends AbstractOperation<V> {
	private static final long serialVersionUID = -918401158100309347L;

	public DeleteOperation(String key) {
		super(null, key);
	}

	public DeleteOperation<V> copy() {
		return new DeleteOperation<V>(this.key);
	}

	public OperationResult<V> call() throws Exception {
		try {
			store.delete(key);
			OperationResult<V> result = new DefaultOperationResult<V>(this,
					node, null);
			return result;
		} finally {
		}
	}

}
