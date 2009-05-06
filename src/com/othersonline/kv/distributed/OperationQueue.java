package com.othersonline.kv.distributed;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public interface OperationQueue {
	public <V> Future<OperationResult<V>> submit(Operation<V> operation);

	public <V> Future<V> execute(Callable<V> callable);
}
