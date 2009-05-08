package com.othersonline.kv.distributed.test;

import java.util.List;

import com.othersonline.kv.distributed.HashAlgorithm;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeLocator;

public class DummyNodeLocator implements NodeLocator {

	public List<Node> getPreferenceList(final HashAlgorithm hashAlg,
			final String key, final List<Node> nodes, final int count) {
		return nodes;
	}

}
