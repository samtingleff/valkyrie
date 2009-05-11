package com.othersonline.kv.distributed.impl;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextSerializer;
import com.othersonline.kv.distributed.Node;

public class PassthroughContextSerializer implements ContextSerializer {

	public byte[] addContext(byte[] objectData) {
		return objectData;
	}

	public Context<byte[]> extractContext(Node source, int nodeRank,
			String key, byte[] rawData) {
		return new DefaultContext<byte[]>(source, nodeRank, 0, key, rawData);
	}

}
