package com.othersonline.kv.distributed;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

public interface OperationQueue {
	public void setConnectionFactory(ConnectionFactory factory);

	public int getQueueSize();

	public <V> Future<OperationResult<V>> submit(Operation<V> operation)
			throws RejectedExecutionException;
}
