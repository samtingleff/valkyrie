package com.othersonline.kv.distributed.impl;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.Node;

public class DefaultContext<V> implements Context<V> {

	private Node source;

	private int nodeRank;

	private int version;

	private String key;

	private V value;

	public DefaultContext(Node source, int nodeRank, int version, String key,
			V value) {
		this.source = source;
		this.nodeRank = nodeRank;
		this.version = version;
		this.key = key;
		this.value = value;
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
