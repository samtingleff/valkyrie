package com.othersonline.kv.distributed.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationCallback;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;

public class NonPersistentThreadPoolOperationQueue extends
		AbstractThreadPoolOperationQueue implements OperationQueue {
	public NonPersistentThreadPoolOperationQueue() {
		this(null);
	}

	public NonPersistentThreadPoolOperationQueue(
			ConnectionFactory connectionFactory) {
		this(connectionFactory, DEFAULT_THREAD_POOL_COUNT,
				DEFAULT_MAX_QUEUE_DEPTH);
	}

	public NonPersistentThreadPoolOperationQueue(
			ConnectionFactory connectionFactory, int threadPoolCount,
			int maxQueueDepth) {
		super(connectionFactory, threadPoolCount, maxQueueDepth);
	}

	public <V> Future<OperationResult<V>> submit(Operation<V> operation)
			throws RejectedExecutionException {
		return super.execute(new CallbackCallable<V>(operation));
	}

	private class CallbackCallable<V> implements Callable<OperationResult<V>> {
		private Operation<V> op;

		public CallbackCallable(Operation<V> op) {
			this.op = op;
		}

		public OperationResult<V> call() throws Exception {
			OperationResult<V> result = null;
			Exception error = null;
			try {
				Node node = op.getNode();
				KeyValueStore store = connectionFactory.getStore(node);
				Callable<OperationResult<V>> delegate = op.getCallable(store);
				result = delegate.call();
			} catch (Exception e) {
				error = e;
			} finally {
				OperationCallback<V> callback = op.getCallback();
				if (callback != null) {
					if (error == null)
						callback.success(result);
					else
						callback.error(result, error);
				}
			}
			return result;
		}

	}
}
