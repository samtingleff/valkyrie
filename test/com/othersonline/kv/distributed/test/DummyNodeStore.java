package com.othersonline.kv.distributed.test;

import java.util.Arrays;
import java.util.List;

import com.othersonline.kv.distributed.DefaultNodeImpl;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeStore;

public class DummyNodeStore implements NodeStore {

	public List<Node> getActiveNodes() {
		return Arrays.asList(new Node[] { new DefaultNodeImpl(0, 0,
				"tcp://localhost:1978?socketTimeout=200&maxActive=20", Arrays
						.asList(new Integer[] { new Integer(200) })) });
	}

}
