package com.othersonline.kv.distributed.impl;

import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationResult;

public class DefaultOperationResult<V> implements OperationResult<V> {
	private Operation<V> operation;

	private Node node;

	private int nodeRank;

	private V value;

	public DefaultOperationResult(Operation<V> operation, Node node,
			int nodeRank, V value) {
		this.operation = operation;
		this.node = node;
		this.nodeRank = nodeRank;
		this.value = value;
	}

	public Node getNode() {
		return node;
	}

	public int getNodeRank() {
		return nodeRank;
	}

	public V getValue() {
		return value;
	}

	public Operation<V> getOperation() {
		return operation;
	}

}
