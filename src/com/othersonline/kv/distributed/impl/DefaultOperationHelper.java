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

public class DefaultOperationHelper {

	public <V> ResultsCollecter<OperationResult<V>> call(
			OperationQueue operationQueue, Operation<V> operation,
			List<Node> nodeList, int requiredResponses, long operationTimeout)
			throws InsufficientResponsesException {
		long start = System.currentTimeMillis();

		LinkedList<Future<OperationResult<V>>> futures = new LinkedList<Future<OperationResult<V>>>();

		AtomicInteger successCounter = new AtomicInteger(0);
		ResultsCollecter<OperationResult<V>> resultCollecter = new ResultsCollecter<OperationResult<V>>(
				nodeList.size());
		CountDownLatch latch = new CountDownLatch(requiredResponses);
		OperationCallback<V> callback = new OperationCallback<V>() {

			private CountDownLatch latch;

			private AtomicInteger successCounter;

			private ResultsCollecter<OperationResult<V>> resultCollecter;

			public OperationCallback<V> init(CountDownLatch latch,
					AtomicInteger successCounter,
					ResultsCollecter<OperationResult<V>> resultCollecter) {
				this.latch = latch;
				this.successCounter = successCounter;
				this.resultCollecter = resultCollecter;
				return this;
			}

			public void success(OperationResult<V> result) {
				resultCollecter.add(result);
				successCounter.incrementAndGet();
				latch.countDown();
			}

			public void error(OperationResult<V> result, Exception e) {
				resultCollecter.add(result);
				latch.countDown();
			}
		}.init(latch, successCounter, resultCollecter);
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

					} catch (ExecutionException e) {

					} catch (Exception e) {

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
