package com.rubiconproject.oss.kv.distributed;

import java.util.List;

public interface NodeChangeListener {
	public void setActiveNodes(List<Node> nodes);
}
