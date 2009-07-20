package com.othersonline.kv.distributed;

public interface ContextSerializer {

	public byte[] addContext(byte[] objectData);

	public Context<byte[]> extractContext(OperationResult<byte[]> bytes);
}
