package com.othersonline.kv.distributed.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.backends.ConnectionFactory;
import com.othersonline.kv.backends.UriConnectionFactory;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationCallback;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;
import com.othersonline.kv.distributed.OperationStatus;

public class NonPersistentThreadPoolOperationQueue extends
		AbstractThreadPoolOperationQueue implements OperationQueue {
	public NonPersistentThreadPoolOperationQueue() {
		this(new UriConnectionFactory());
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
			Node node = null;
			long start = System.currentTimeMillis();
			try {
				node = op.getNode();
				KeyValueStore store = connectionFactory.getStore(node
						.getConnectionURI());
				Callable<OperationResult<V>> delegate = op.getCallable(store);
				result = delegate.call();
			} catch (Exception e) {
				result = new DefaultOperationResult<V>(op, null,
						OperationStatus.Error, System.currentTimeMillis()
								- start, e);
			} finally {
				OperationCallback<V> callback = op.getCallback();
				if (callback != null) {
					callback.completed(result);
				}
			}
			return result;
		}

	}
}
