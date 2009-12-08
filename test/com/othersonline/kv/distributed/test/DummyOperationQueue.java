package com.othersonline.kv.distributed.test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.backends.ConnectionFactory;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationCallback;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;
import com.othersonline.kv.distributed.OperationStatus;
import com.othersonline.kv.distributed.impl.DefaultOperationResult;

public class DummyOperationQueue implements OperationQueue {
	private ConnectionFactory connectionFactory;

	public DummyOperationQueue(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public void setConnectionFactory(ConnectionFactory factory) {
		this.connectionFactory = factory;
	}

	public void start() {
	}

	public void stop() {
	}

	public int getQueueSize() {
		return 0;
	}

	public <V> Future<OperationResult<V>> submit(Operation<V> operation) {
		Node node = null;
		OperationResult<V> result = null;
		long start = System.currentTimeMillis();
		try {
			node = operation.getNode();
			KeyValueStore store = connectionFactory.getStore(null, node.getConnectionURI());
			Callable<OperationResult<V>> callable = operation
					.getCallable(store);
			result = callable.call();
			Future<OperationResult<V>> future = new DummyFuture<OperationResult<V>>(
					result, null);
			result = future.get();
			return future;
		} catch (Exception e) {
			e.printStackTrace();
			result = new DefaultOperationResult<V>(operation, null, OperationStatus.Error, System.currentTimeMillis() - start, e);
			throw new RuntimeException(e);
		} finally {
			OperationCallback<V> callback = operation.getCallback();
			if (callback != null)
					callback.completed(result);
		}
	}

	private static class DummyFuture<V> implements Future<V> {
		private V v;

		private Exception error;

		public DummyFuture(V v, Exception error) {
			this.v = v;
			this.error = error;
		}

		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		public V get() throws InterruptedException, ExecutionException {
			if (error != null)
				throw new ExecutionException(error);
			else
				return v;
		}

		public V get(long timeout, TimeUnit unit) throws InterruptedException,
				ExecutionException, TimeoutException {
			if (error != null)
				throw new ExecutionException(error);
			else
				return v;
		}

		public boolean isCancelled() {
			return false;
		}

		public boolean isDone() {
			return true;
		}
	}
}
