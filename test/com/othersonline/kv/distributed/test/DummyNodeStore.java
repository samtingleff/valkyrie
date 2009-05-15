package com.othersonline.kv.distributed.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.othersonline.kv.distributed.AbstractRefreshingNodeStore;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeStore;
import com.othersonline.kv.distributed.impl.DefaultNodeImpl;

public class DummyNodeStore extends AbstractRefreshingNodeStore implements
		NodeStore {

	private volatile List<Node> nodes = Arrays
			.asList(new Node[] { new DefaultNodeImpl(1, 1, "salt",
					"tcp://stanley:1978?socketTimeout=200&maxActive=20", Arrays
							.asList(new Integer[] { new Integer(200) })) });

	public DummyNodeStore() {
	}

	public DummyNodeStore(List<Node> nodes) {
		this.nodes = nodes;
	}

	public List<Node> getActiveNodes() {
		return nodes;
	}

	public void addNode(Node n) {
		List<Node> newNodes = new ArrayList<Node>(nodes.size() + 1);
		for (Node x : nodes) {
			newNodes.add(x);
		}
		newNodes.add(n);
		nodes = newNodes;
		publish();
	}

	public void removeNode(Node n) {
		nodes.remove(n);
	}
}
