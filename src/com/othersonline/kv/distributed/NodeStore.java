package com.othersonline.kv.distributed;

import java.util.List;

public interface NodeStore {
	public List<Node> getActiveNodes();

	public void addNode(Node node);

	public void removeNode(Node node);

	public void addChangeListener(NodeChangeListener listener);
}
