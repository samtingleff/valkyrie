package com.othersonline.kv.distributed.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;

public abstract class AbstractThreadPoolOperationQueue implements
		OperationQueue {
	protected static final int DEFAULT_THREAD_POOL_COUNT = 5;

	protected static final int DEFAULT_MAX_QUEUE_DEPTH = 100;

	protected Log log = LogFactory.getLog(getClass());

	protected ExecutorService executor;

	protected ConnectionFactory connectionFactory;

	protected int threadPoolCount = DEFAULT_THREAD_POOL_COUNT;

	protected int maxQueueDepth = DEFAULT_MAX_QUEUE_DEPTH;

	public AbstractThreadPoolOperationQueue(ConnectionFactory connectionFactory) {
		this(connectionFactory, DEFAULT_THREAD_POOL_COUNT,
				DEFAULT_MAX_QUEUE_DEPTH);
	}

	public AbstractThreadPoolOperationQueue(
			ConnectionFactory connectionFactory, int threadPoolCount,
			int maxQueueDepth) {
		this.connectionFactory = connectionFactory;
		this.threadPoolCount = threadPoolCount;
		this.maxQueueDepth = maxQueueDepth;
	}

	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public void setThreadPoolCount(int threadPoolCount) {
		this.threadPoolCount = threadPoolCount;
	}

	public void setMaxQueueDepth(int maxQueueDepth) {
		this.maxQueueDepth = maxQueueDepth;
	}

	public OperationQueue start() {
		executor = new ThreadPoolExecutor(threadPoolCount, threadPoolCount, 0l,
				TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(
						maxQueueDepth));
		return this;
	}

	public void stop() {
		executor.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!executor.awaitTermination(2000l, TimeUnit.MILLISECONDS)) {
				executor.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!executor.awaitTermination(2000l, TimeUnit.MILLISECONDS))
					log.error("Pool did not terminate within timeout");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

	public int getQueueSize() {
		return 100;
	}

	/**
	 * Execute a Callable. Throws RejectedExecutionException, probably when
	 * queue size is too large.
	 * 
	 * @param <V>
	 * @param callable
	 * @return
	 * @throws RejectedExecutionException
	 */
	protected <V> Future<V> execute(Callable<V> callable)
			throws RejectedExecutionException {
		return executor.submit(callable);
	}

	public abstract <V> Future<OperationResult<V>> submit(Operation<V> operation);

}