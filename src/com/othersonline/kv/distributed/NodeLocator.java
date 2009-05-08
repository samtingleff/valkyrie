package com.othersonline.kv.distributed;

import java.util.List;

public interface NodeLocator {
	public List<Node> getPreferenceList(final String key,
			final List<Node> nodes, final int count);
}
