package com.othersonline.kv.distributed.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.othersonline.kv.distributed.InsufficientResponsesException;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationCallback;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;
import com.othersonline.kv.distributed.OperationStatus;

public class DefaultOperationHelper {

	private OperationLog operationLog = OperationLog.getInstance();

	public <V> ResultsCollecter<OperationResult<V>> call(
			OperationQueue operationQueue, Operation<V> operation,
			List<Node> nodeList, int requiredResponses, long operationTimeout,
			boolean considerNullAsSuccess)
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
			op.setNodeRank(i);
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
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (NoSuchElementException e) {
				break;
			}
		}
		if (successCounter.get() < requiredResponses) {
			throw new InsufficientResponsesException(requiredResponses,
					successCounter.get());
		}
		return resultCollecter;
	}
}
