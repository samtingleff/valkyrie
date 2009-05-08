package com.othersonline.kv.distributed.test;

import java.util.List;

import com.othersonline.kv.distributed.HashAlgorithm;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeLocator;
import com.othersonline.kv.distributed.NodeStore;

public class DummyNodeLocator implements NodeLocator {
	private NodeStore nodeStore;

	public DummyNodeLocator(NodeStore nodeStore) {
		this.nodeStore = nodeStore;
	}

	public void setNodeStore(NodeStore store) {
		this.nodeStore = store;
	}

	public List<Node> getPreferenceList(HashAlgorithm hashAlg, String key,
			int count) {
		return nodeStore.getActiveNodes();
	}

}
