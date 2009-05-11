package com.othersonline.kv.distributed.impl;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextSerializer;
import com.othersonline.kv.distributed.Node;

public class PassthroughContextSerializer implements ContextSerializer {

	public byte[] addContext(byte[] objectData) {
		return objectData;
	}

	public Context<byte[]> extractContext(Node source, byte[] rawData) {
		return new DefaultContext<byte[]>(0, rawData);
	}

}
