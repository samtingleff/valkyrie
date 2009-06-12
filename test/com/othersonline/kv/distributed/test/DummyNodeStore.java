package com.othersonline.kv.distributed.test;

import java.util.List;
import java.util.Properties;

import com.othersonline.kv.distributed.AbstractRefreshingNodeStore;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeStore;
import com.othersonline.kv.distributed.impl.DefaultNodeImpl;

public class DummyNodeStore extends AbstractRefreshingNodeStore implements
		NodeStore {
	public DummyNodeStore() {
		this.activeNodes.add(new DefaultNodeImpl(1, 1, "salt:1:1",
				"hash://localhost?id=1"));
		this.activeNodes.add(new DefaultNodeImpl(2, 2, "salt:2:2",
				"hash://localhost?id=2"));
		this.activeNodes.add(new DefaultNodeImpl(3, 3, "salt:3:3",
				"hash://localhost?id=3"));
	}

	public DummyNodeStore(List<Node> nodes) {
		this.activeNodes = nodes;
	}

	public void setProperties(Properties props) {
	}

	public List<Node> refreshActiveNodes() {
		return activeNodes;
	}
}
