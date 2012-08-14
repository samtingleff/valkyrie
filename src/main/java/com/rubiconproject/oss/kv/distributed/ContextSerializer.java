package com.rubiconproject.oss.kv.distributed;

public interface ContextSerializer {

	public byte[] addContext(byte[] objectData);

	public Context<byte[]> extractContext(OperationResult<byte[]> bytes);
	public BulkContext<byte[]> extractBulkContext(BulkOperationResult<byte[]> bytes);
}
