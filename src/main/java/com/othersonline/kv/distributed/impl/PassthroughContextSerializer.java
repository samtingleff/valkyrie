package com.othersonline.kv.distributed.impl;

import com.othersonline.kv.distributed.BulkContext;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextSerializer;
import com.othersonline.kv.distributed.OperationResult;
import com.othersonline.kv.distributed.BulkOperationResult;

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
