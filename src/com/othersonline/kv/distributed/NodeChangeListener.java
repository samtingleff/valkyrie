package com.othersonline.kv.distributed;

import java.util.List;

public interface NodeChangeListener {
	public void activeNodes(List<Node> nodes);
}
