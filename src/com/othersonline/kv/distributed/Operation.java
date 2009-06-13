package com.othersonline.kv.distributed;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.transcoder.Transcoder;

public interface Operation<V> extends Serializable {
	public String getName();

	public String getKey();

	public Operation<V> copy();

	public Transcoder getTranscoder();

	public void setTranscoder(Transcoder transcoder);

	public OperationCallback<V> getCallback();

	public void setCallback(OperationCallback<V> callback);

	public Node getNode();

	public void setNode(Node node);

	public int getNodeRank();

	public void setNodeRank(int index);

	public Callable<OperationResult<V>> getCallable(KeyValueStore store);
}
