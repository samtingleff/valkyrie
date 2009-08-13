package com.othersonline.kv.distributed;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractRefreshingNodeStore implements NodeStore {
	public static final Long DEFAULT_DELAY = 1000l * 60l;

	public static final Long DEFAULT_PERIOD = 1000l * 60l;

	protected Log log = LogFactory.getLog(getClass());

	protected List<NodeChangeListener> listeners = new LinkedList<NodeChangeListener>();

	protected volatile List<Node> activeNodes = new LinkedList<Node>();

	public void start() throws IOException, ConfigurationException {
		this.activeNodes = refreshActiveNodes();
		publish();
		schedule(DEFAULT_DELAY, DEFAULT_PERIOD);
	}

	public void schedule(long delay, long period) {
		Timer t = new Timer(true);
		t.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				try {
					List<Node> newActiveNodes = refreshActiveNodes();
					if (changed(activeNodes, newActiveNodes)) {
						activeNodes = newActiveNodes;
						publish();
					}
				} catch (Exception e) {
					log.error("Exception calling refreshActiveNodes()", e);
				}
			}
		}, delay, period);
	}

	public void addChangeListener(NodeChangeListener listener) {
		this.listeners.add(listener);
	}

	public void addNode(Node node) {
		if (!this.activeNodes.contains(node)) {
			this.activeNodes.add(node);
			publish();
		}
	}

	public void removeNode(Node node) {
		if (this.activeNodes.contains(node)) {
			this.activeNodes.remove(node);
			publish();
		}
	}

	public List<Node> getActiveNodes() {
		return activeNodes;
	}

	public abstract List<Node> refreshActiveNodes() throws IOException,
			ConfigurationException;

	protected void publish() {
		activeNodes = getActiveNodes();
		for (NodeChangeListener listener : listeners) {
			try {
				listener.setActiveNodes(activeNodes);
			} catch (Exception e) {
				log.warn("Exception calling activeNodes() on listener class.",
						e);
			}
		}
	}

	private boolean changed(List<Node> active, List<Node> updated) {
		if (active.size() != updated.size())
			return true;
		for (int i = 0; i < active.size(); ++i) {
			Node currentNode = active.get(i);
			Node updatedNode = updated.get(i);
			// compare
			if ((currentNode.getId() != updatedNode.getId())
					|| (!currentNode.getConnectionURI().equals(
							updatedNode.getConnectionURI()))
					|| (currentNode.getPhysicalId() != updatedNode
							.getPhysicalId())
					|| (!currentNode.getSalt().equals(updatedNode.getSalt())))
				return true;
		}
		return false;
	}
}
