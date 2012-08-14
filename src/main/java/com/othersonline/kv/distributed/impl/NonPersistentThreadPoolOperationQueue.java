package com.othersonline.kv.distributed.impl;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

	protected Log operationLog = LogFactory.getLog("haymitch.backendlog");

	public NonPersistentThreadPoolOperationQueue(Map defaultProperties) {
		this(defaultProperties, new UriConnectionFactory());
	}

	public NonPersistentThreadPoolOperationQueue(Map defaultProperties,
			ConnectionFactory connectionFactory) {
		this(defaultProperties, connectionFactory, DEFAULT_THREAD_POOL_COUNT,
				DEFAULT_MAX_QUEUE_DEPTH);
	}

	public NonPersistentThreadPoolOperationQueue(Map defaultProperties,
			ConnectionFactory connectionFactory, int threadPoolCount,
			int maxQueueDepth) {
		super(defaultProperties, connectionFactory, threadPoolCount,
				maxQueueDepth);
	}

	public <V> Future<OperationResult<V>> submit(Operation<V> operation)
			throws RejectedExecutionException {
		return super.execute(new CallbackCallable<V>(operation));
	}

	private class CallbackCallable<V> implements Callable<OperationResult<V>> {
		private long enqueueTime;

		private Operation<V> op;

		public CallbackCallable(Operation<V> op) {
			this.op = op;
			this.enqueueTime = System.currentTimeMillis();
		}

		public OperationResult<V> call() throws Exception {
			OperationResult<V> result = null;
			Node node = null;
			long start = System.currentTimeMillis();
			try {
				node = op.getNode();
				KeyValueStore store = connectionFactory.getStore(
						defaultProperties, node.getConnectionURI());
				Callable<OperationResult<V>> delegate = op.getCallable(store);
				result = delegate.call();
			} catch (Exception e) {
				log.error("Exception fetching node", e);
				result = new DefaultOperationResult<V>(op, null,
						OperationStatus.Error, System.currentTimeMillis()
								- start, e);
			} finally {
				try {
					operationLog.info(String.format(
							"%1$s_%2$s_%3$d %4$dms queue_time=%5$dms", op
									.getName(), result.getStatus().toString()
									.toLowerCase(), node.getId(), result
									.getDuration(), start - enqueueTime));
				} catch (Exception e) {
					log.error("Exception writing to operation log", e);
				}
				OperationCallback<V> callback = op.getCallback();
				if (callback != null) {
					callback.completed(result);
				}
			}
			return result;
		}

	}
}
