package com.othersonline.kv.distributed.impl;

import java.util.concurrent.Callable;

import com.othersonline.kv.distributed.AbstractOperation;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationResult;
import com.othersonline.kv.distributed.OperationStatus;
import com.othersonline.kv.transcoder.Transcoder;

public class GetOperation<V> extends AbstractOperation<V> implements
		Operation<V>, Callable<OperationResult<V>> {
	private static final long serialVersionUID = -3908847991063100534L;

	public GetOperation(Transcoder transcoder, String key) {
		super(transcoder, key);
	}

	public String getName() {
		return "get";
	}

	public GetOperation<V> copy() {
		return new GetOperation<V>(this.transcoder, this.key);
	}

	public OperationResult<V> call() throws Exception {
		try {
			long start = System.currentTimeMillis();
			V v = (transcoder == null) ? (V) store.get(key) : (V) store.get(
					key, transcoder);
			OperationResult<V> result = new DefaultOperationResult<V>(this, v,
					(v == null) ? OperationStatus.NullValue
							: OperationStatus.Success, System
							.currentTimeMillis()
							- start, null);
			return result;
		} finally {
		}
	}

}
