package com.othersonline.kv.distributed.test;

import java.util.Arrays;
import java.util.List;

import com.othersonline.kv.distributed.AbstractRefreshingNodeStore;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeStore;
import com.othersonline.kv.distributed.impl.DefaultNodeImpl;

public class DummyNodeStore extends AbstractRefreshingNodeStore implements
		NodeStore {

	private List<Node> nodes = Arrays.asList(new Node[] { new DefaultNodeImpl(
			1, 1, "tcp://localhost:1978?socketTimeout=200&maxActive=20", Arrays
					.asList(new Integer[] { new Integer(200) })) });

	public DummyNodeStore() {
	}

	public DummyNodeStore(List<Node> nodes) {
		this.nodes = nodes;
	}

	public List<Node> getActiveNodes() {
		return nodes;
	}
}
