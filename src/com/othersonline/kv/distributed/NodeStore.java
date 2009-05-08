package com.othersonline.kv.distributed;

import java.util.List;

public interface NodeStore {
	public List<Node> getActiveNodes();

	public void addChangeListener(NodeChangeListener listener);
}
