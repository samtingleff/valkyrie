package com.rubiconproject.oss.kv.distributed.impl;

import com.rubiconproject.oss.kv.distributed.AbstractOperation;
import com.rubiconproject.oss.kv.distributed.OperationResult;
import com.rubiconproject.oss.kv.distributed.OperationStatus;

public class DeleteOperation<V> extends AbstractOperation<V> {
	private static final long serialVersionUID = -918401158100309347L;

	public String getName() {
		return "delete";
	}

	public DeleteOperation(String key) {
		super(null, key);
	}

	public DeleteOperation<V> copy() {
		return new DeleteOperation<V>(this.key);
	}

	public OperationResult<V> call() throws Exception {
		try {
			long start = System.currentTimeMillis();
			store.delete(key);
			OperationResult<V> result = new DefaultOperationResult<V>(this,
					null, OperationStatus.Success, System.currentTimeMillis()
							- start, null);
			return result;
		} finally {
		}
	}

}
