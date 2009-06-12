package com.othersonline.kv.distributed.test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationCallback;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;

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

	public int getQueueSize() {
		return 0;
	}

	public <V> Future<OperationResult<V>> submit(Operation<V> operation) {
		OperationResult<V> result = null;
		Exception e = null;
		try {
			Node node = operation.getNode();
			KeyValueStore store = connectionFactory.getStore(node);
			Callable<OperationResult<V>> callable = operation
					.getCallable(store);
			result = callable.call();
			Future<OperationResult<V>> future = new DummyFuture<OperationResult<V>>(
					result, null);
			result = future.get();
			return future;
		} catch (Exception e1) {
			e.printStackTrace();
			e = e1;
			throw new RuntimeException(e);
		} finally {
			OperationCallback<V> callback = operation.getCallback();
			if (callback != null) {
				if (e == null)
					callback.success(result);
				else
					callback.error(result, e);
			}
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
