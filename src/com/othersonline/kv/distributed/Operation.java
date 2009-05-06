package com.othersonline.kv.distributed;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.othersonline.kv.KeyValueStore;

public interface Operation<V> extends Serializable {
	public Operation<V> copy();

	public OperationCallback<V> getCallback();

	public void setCallback(OperationCallback<V> callback);

	public Node getNode();

	public void setNode(Node node);

	public Callable<OperationResult<V>> getCallable(KeyValueStore store);
}
