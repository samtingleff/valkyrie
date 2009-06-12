package com.othersonline.kv.distributed;

import java.util.List;

public interface NodeLocator {
	public void setActiveNodes(List<Node> nodes);

	public int getPrimaryNode(HashAlgorithm hashAlg, String key);

	public List<Node> getPreferenceList(final HashAlgorithm hashAlg,
			final String key, final int count);
}
