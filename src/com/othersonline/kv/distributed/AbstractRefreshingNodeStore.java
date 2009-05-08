package com.othersonline.kv.distributed;

import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractRefreshingNodeStore implements NodeStore {
	public static final Long DEFAULT_DELAY = 0l;

	public static final Long DEFAULT_PERIOD = 1000l * 60l;

	private Log log = LogFactory.getLog(getClass());

	private List<NodeChangeListener> listeners = new LinkedList<NodeChangeListener>();

	private volatile List<Node> activeNodes = null;

	public void schedule(long delay, long period) {
		Timer t = new Timer(true);
		t.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				activeNodes = getActiveNodes();
				for (NodeChangeListener listener : listeners) {
					try {
						listener.activeNodes(activeNodes);
					} catch (Exception e) {
						log
								.warn(
										"Exception calling activeNodes() on listener class.",
										e);
					}
				}
			}
		}, delay, period);
	}

	public void addChangeListener(NodeChangeListener listener) {
		this.listeners.add(listener);
	}

	public void addNode(Node node) {
		this.activeNodes.add(node);
	}

	public void removeNode(Node node) {
		this.activeNodes.remove(node);
	}

	public abstract List<Node> getActiveNodes();
}
