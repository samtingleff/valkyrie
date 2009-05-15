package com.othersonline.kv.distributed.test;

import java.util.List;

import com.othersonline.kv.distributed.AbstractRefreshingNodeStore;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeStore;

public class DummyNodeStore extends AbstractRefreshingNodeStore implements
		NodeStore {
	public DummyNodeStore(List<Node> nodes) {
		this.activeNodes = nodes;
	}

	public List<Node> refreshActiveNodes() {
		return activeNodes;
	}
}
