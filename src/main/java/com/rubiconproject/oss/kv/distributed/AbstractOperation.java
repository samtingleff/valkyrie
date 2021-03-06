package com.rubiconproject.oss.kv.distributed;

import java.util.concurrent.Callable;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

public abstract class AbstractOperation<V> implements Operation<V>,
		Callable<OperationResult<V>> {
	private static final long serialVersionUID = -1366881371899551925L;

	protected transient KeyValueStore store;

	protected transient Transcoder transcoder;

	protected transient OperationCallback<V> callback;

	protected Node node;

	protected int nodeRank;

	protected String key;

	public AbstractOperation(Transcoder transcoder, String key) {
		this.transcoder = transcoder;
		this.key = key;
	}

	public OperationCallback<V> getCallback() {
		return callback;
	}

	public void setCallback(OperationCallback<V> callback) {
		this.callback = callback;
	}

	public Transcoder getTranscoder() {
		return transcoder;
	}

	public void setTranscoder(Transcoder transcoder) {
		this.transcoder = transcoder;
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public int getNodeRank() {
		return nodeRank;
	}

	public void setNodeRank(int nodeRank) {
		this.nodeRank = nodeRank;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Callable<OperationResult<V>> getCallable(KeyValueStore store) {
		this.store = store;
		return this;
	}

}
