package com.rubiconproject.oss.kv.distributed.impl;

import com.rubiconproject.oss.kv.distributed.BulkContext;
import com.rubiconproject.oss.kv.distributed.BulkOperationResult;
import com.rubiconproject.oss.kv.distributed.Context;
import com.rubiconproject.oss.kv.distributed.ContextSerializer;
import com.rubiconproject.oss.kv.distributed.OperationResult;

public class PassthroughContextSerializer implements ContextSerializer {

	public byte[] addContext(byte[] objectData) {
		return objectData;
	}

	public Context<byte[]> extractContext(OperationResult<byte[]> result) {
		return new DefaultContext<byte[]>(result, result.getOperation()
				.getNode(), result.getOperation().getNodeRank(), 0, result
				.getOperation().getKey(), result.getValue());
	}

	public BulkContext<byte[]> extractBulkContext(BulkOperationResult<byte[]> result) {
		return new DefaultBulkContext<byte[]>( 
				result,
				result.getOperation().getNode(),
				result.getOperation().getNodeRank(),
				0,
				result.getBulkOperation().getKeys(),
				result.getValues());
	}

	
}
