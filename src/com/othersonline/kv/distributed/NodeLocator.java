package com.othersonline.kv.distributed;

import java.util.List;

public interface NodeLocator {
	public void setNodeStore(NodeStore store);

	public List<Node> getPreferenceList(final HashAlgorithm hashAlg,
			final String key, final int count);
}
