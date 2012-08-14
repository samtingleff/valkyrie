package com.othersonline.kv.distributed;

import java.util.List;

public interface NodeChangeListener {
	public void setActiveNodes(List<Node> nodes);
}
