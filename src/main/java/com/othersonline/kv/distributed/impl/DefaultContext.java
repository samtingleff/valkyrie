package com.othersonline.kv.distributed.impl;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.OperationResult;

public class DefaultContext<V> implements Context<V> {

	private OperationResult<V> result;

	private Node source;

	private int nodeRank;

	private int version;

	private String key;

	private V value;

	public DefaultContext(OperationResult<V> result, Node source, int nodeRank,
			int version, String key, V value) {
		this.result = result;
		this.source = source;
		this.nodeRank = nodeRank;
		this.version = version;
		this.key = key;
		this.value = value;
	}

	public OperationResult<V> getResult() {
		return result;
	}

	public Node getSourceNode() {
		return source;
	}

	public int getNodeRank() {
		return nodeRank;
	}

	public int getVersion() {
		return version;
	}

	public String getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

}
