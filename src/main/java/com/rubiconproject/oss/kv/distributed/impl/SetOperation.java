package com.rubiconproject.oss.kv.distributed.impl;

import java.util.concurrent.Callable;

import com.rubiconproject.oss.kv.distributed.AbstractOperation;
import com.rubiconproject.oss.kv.distributed.Operation;
import com.rubiconproject.oss.kv.distributed.OperationResult;
import com.rubiconproject.oss.kv.distributed.OperationStatus;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

public class SetOperation<V> extends AbstractOperation<V> implements
		Operation<V>, Callable<OperationResult<V>> {
	private static final long serialVersionUID = -6746808553560914494L;

	private V value;

	public SetOperation(Transcoder transcoder, String key, V value) {
		super(transcoder, key);
		this.value = value;
	}

	public String getName() {
		return "set";
	}

	public SetOperation<V> copy() {
		return new SetOperation<V>(this.transcoder, this.key, this.value);
	}

	public OperationResult<V> call() throws Exception {
		try {
			long start = System.currentTimeMillis();
			if (transcoder == null)
				store.set(key, value);
			else
				store.set(key, value, transcoder);
			OperationResult<V> result = new DefaultOperationResult<V>(this,
					null, OperationStatus.Success, System.currentTimeMillis()
							- start, null);
			return result;
		} finally {
		}
	}
}
