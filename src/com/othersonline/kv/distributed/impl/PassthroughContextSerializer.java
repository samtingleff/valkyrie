package com.othersonline.kv.distributed.impl;

import java.util.Collections;
import java.util.List;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextSerializer;
import com.othersonline.kv.distributed.ExtractedContext;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;

public class PassthroughContextSerializer implements ContextSerializer {

	public byte[] addContext(byte[] objectData) {
		return objectData;
	}

	public ExtractedContext<byte[]> extractContext(Node source, byte[] rawData) {
		return new NullExtractedContext<byte[]>(rawData);
	}

	private static class NullExtractedContext<V> implements
			ExtractedContext<V>, Context<V> {

		private V value;

		public NullExtractedContext(V value) {
			this.value = value;
		}

		public List<Operation<V>> getAdditionalOperations() {
			return Collections.EMPTY_LIST;
		}

		public Context<V> getContext() {
			return this;
		}

		public V getValue() {
			return value;
		}

		public void setValue(V value) {
			this.value = value;
		}

		public int getVersion() {
			return 0;
		}

	}

}
