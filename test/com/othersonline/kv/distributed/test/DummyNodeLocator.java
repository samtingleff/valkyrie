package com.othersonline.kv.distributed.test;

import java.util.Arrays;
import java.util.List;

import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeLocator;

public class DummyNodeLocator implements NodeLocator {

	public List<Node> getPreferenceList(long hashCode, int count) {
		return Arrays.asList(new Node[] { new Node() {
			public String getConnectionURI() {
				return "tcp://localhost:1978";
			}

			public int getId() {
				return 0;
			} } });
	}

}
