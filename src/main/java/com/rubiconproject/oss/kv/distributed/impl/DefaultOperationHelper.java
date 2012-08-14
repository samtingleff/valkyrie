package com.rubiconproject.oss.kv.distributed.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rubiconproject.oss.kv.distributed.InsufficientResponsesException;
import com.rubiconproject.oss.kv.distributed.Node;
import com.rubiconproject.oss.kv.distributed.Operation;
import com.rubiconproject.oss.kv.distributed.OperationCallback;
import com.rubiconproject.oss.kv.distributed.OperationQueue;
import com.rubiconproject.oss.kv.distributed.OperationResult;
import com.rubiconproject.oss.kv.distributed.OperationStatus;

public class DefaultOperationHelper {

	private OperationLog operationLog = OperationLog.getInstance();

	private Log log = LogFactory.getLog(getClass());

	public <V> ResultsCollecter<OperationResult<V>> call(
			OperationQueue operationQueue, Operation<V> operation,
			List<Node> nodeList, int nodeRankOffset, int requiredResponses,
			long operationTimeout, boolean considerNullAsSuccess,
			boolean throwInsufficientResponsesException)
			throws InsufficientResponsesException {
		long start = System.currentTimeMillis();

		operationLog.logPreferenceList(operation.getKey(), nodeList);

		LinkedList<Future<OperationResult<V>>> futures = new LinkedList<Future<OperationResult<V>>>();

		AtomicInteger successCounter = new AtomicInteger(0);
		ResultsCollecter<OperationResult<V>> resultCollecter = new ResultsCollecter<OperationResult<V>>(
				nodeList.size());
		CountDownLatch latch = new CountDownLatch(requiredResponses);
		OperationCallback<V> callback = new OperationCallback<V>() {

			private CountDownLatch latch;

			private AtomicInteger successCounter;

			private boolean considerNullAsSuccess;

			private ResultsCollecter<OperationResult<V>> resultCollecter;

			public OperationCallback<V> init(CountDownLatch latch,
					AtomicInteger successCounter,
					boolean considerNullAsSuccess,
					ResultsCollecter<OperationResult<V>> resultCollecter) {
				this.latch = latch;
				this.successCounter = successCounter;
				this.considerNullAsSuccess = considerNullAsSuccess;
				this.resultCollecter = resultCollecter;
				return this;
			}

			public void completed(OperationResult<V> result) {
				resultCollecter.add(result);
				latch.countDown();
				if (result.getStatus().equals(OperationStatus.Success)) {
					successCounter.incrementAndGet();
				}
				if ((result.getStatus().equals(OperationStatus.NullValue))
						&& (considerNullAsSuccess)) {
					successCounter.incrementAndGet();
				}
			}
		}.init(latch, successCounter, considerNullAsSuccess, resultCollecter);
		for (int i = 0; i < nodeList.size(); ++i) {
			Operation<V> op = operation.copy();
			op.setCallback(callback);
			op.setNode(nodeList.get(i));
			op.setNodeRank(i + nodeRankOffset);
			Future<OperationResult<V>> future = operationQueue.submit(op);
			futures.add(future);
		}
		try {
			latch.await(operationTimeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		// stop waiting if any of the following occur
		// 1) successful response from r/w nodes
		// 2) response completes successfully or not from n nodes
		// 3) timeout exceeded
		while ((successCounter.get() < requiredResponses)
				&& (resultCollecter.size() < nodeList.size())
				&& ((System.currentTimeMillis() - start) < operationTimeout)) {
			try {
				Future<OperationResult<V>> future = futures.pop();
				if (future != null) {
					try {
						OperationResult<V> result = future.get(operationTimeout
								- (System.currentTimeMillis() - start),
								TimeUnit.MILLISECONDS);
					} catch (TimeoutException e) {
						log.info("TimeoutException waiting on response", e);
					} catch (ExecutionException e) {
						log.info("ExecutionException waiting on response", e);
					} catch (Exception e) {
						log.info("Exception waiting on response", e);
					}
				}
			} catch (NoSuchElementException e) {
				break;
			}
		}
		if ((successCounter.get() < requiredResponses)
				&& (throwInsufficientResponsesException)) {
			throw new InsufficientResponsesException(requiredResponses,
					successCounter.get());
		}
		return resultCollecter;
	}
}
