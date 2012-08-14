package com.rubiconproject.oss.kv.distributed;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public interface NodeStore {
	public void setProperties(Properties props);

	public void start() throws IOException, ConfigurationException;

	public List<Node> getActiveNodes();

	public void addNode(Node node);

	public void removeNode(Node node);

	public void addChangeListener(NodeChangeListener listener);
}
