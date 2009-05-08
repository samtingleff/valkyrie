package com.othersonline.kv.distributed.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationCallback;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;
import com.othersonline.kv.util.DaemonThreadFactory;

public class NonPersistentThreadPoolOperationQueue implements OperationQueue {
	private static final int DEFAULT_THREAD_POOL_COUNT = 5;

	private ExecutorService executor;

	private ConnectionFactory connectionFactory;

	private int threadPoolCount = DEFAULT_THREAD_POOL_COUNT;

	public NonPersistentThreadPoolOperationQueue() {
		this(null);
	}

	public NonPersistentThreadPoolOperationQueue(
			ConnectionFactory connectionFactory) {
		this(connectionFactory, DEFAULT_THREAD_POOL_COUNT);
	}

	public NonPersistentThreadPoolOperationQueue(
			ConnectionFactory connectionFactory, int threadPoolCount) {
		this.connectionFactory = connectionFactory;
		this.threadPoolCount = threadPoolCount;
	}

	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public OperationQueue start() {
		executor = Executors.newFixedThreadPool(threadPoolCount,
				new DaemonThreadFactory());
		return this;
	}

	public <V> Future<V> execute(Callable<V> callable) {
		return executor.submit(callable);
	}

	public <V> Future<OperationResult<V>> submit(Operation<V> operation) {
		CallbackCallable<V> callable = new CallbackCallable<V>(operation);
		return execute(callable);
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
