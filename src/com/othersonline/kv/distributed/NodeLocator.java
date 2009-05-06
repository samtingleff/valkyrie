package com.othersonline.kv.distributed;

import java.util.List;

public interface NodeLocator {

	public List<Node> getPreferenceList(long hashCode, int count);
}
