package com.othersonline.kv.distributed;

import java.util.ArrayList;
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

import com.othersonline.kv.KeyValueStoreException;

public class DefaultDistributedKeyValueStore implements
		DistributedKeyValueStore {
	private Log log = LogFactory.getLog(getClass());

	private Configuration config;

	private HashAlgorithm hash;

	private NodeLocator nodeLocator;

	private OperationQueue syncOperationQueue;

	private OperationQueue asyncOperationQueue;

	private ConnectionFactory connectionFactory;

	private ContextSerializer contextSerializer;

	public DefaultDistributedKeyValueStore() {
	}

	public void setConfiguration(Configuration config) {
		this.config = config;
	}

	public void setHashAlgorithm(HashAlgorithm hash) {
		this.hash = hash;
	}

	public void setNodeLocator(NodeLocator locator) {
		this.nodeLocator = locator;
	}

	public void setSyncOperationQueue(OperationQueue queue) {
		this.syncOperationQueue = queue;
	}

	public void setAsyncOperationQueue(OperationQueue queue) {
		this.asyncOperationQueue = queue;
	}

	public void setConnectionFactory(ConnectionFactory factory) {
		this.connectionFactory = factory;
	}

	public void setContextSerializer(ContextSerializer contextSerializer) {
		this.contextSerializer = contextSerializer;
	}

	public List<Context<byte[]>> get(String key) throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("get(%1$s)", key));

		long start = System.currentTimeMillis();

		long hashCode = hash.hash(key);

		// ask for a response from n nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(hashCode, config
				.getReplicas());

		// wait for a response from r nodes
		LinkedList<Future<OperationResult<byte[]>>> futures = new LinkedList<Future<OperationResult<byte[]>>>();
		AtomicInteger successCounter = new AtomicInteger(0);
		List<OperationResult<byte[]>> resultCollecter = new ArrayList<OperationResult<byte[]>>(
				futures.size());
		CountDownLatch latch = new CountDownLatch(config.getRequiredReads());
		OperationCallback<byte[]> callback = new OperationCallback<byte[]>() {

			private CountDownLatch latch;

			private AtomicInteger successCounter;

			private List<OperationResult<byte[]>> resultCollecter;

			public OperationCallback<byte[]> init(CountDownLatch latch,
					AtomicInteger successCounter,
					List<OperationResult<byte[]>> resultCollecter) {
				this.latch = latch;
				this.successCounter = successCounter;
				this.resultCollecter = resultCollecter;
				return this;
			}

			public void success(OperationResult<byte[]> result) {
				resultCollecter.add(result);
				latch.countDown();
				successCounter.incrementAndGet();
			}

			public void error(OperationResult<byte[]> result, Exception e) {
				resultCollecter.add(result);
				latch.countDown();
			}
		}.init(latch, successCounter, resultCollecter);
		for (Node node : nodeList) {
			try {
				GetOperation<byte[]> getOperation = new GetOperation<byte[]>(
						callback, node, key);
				Future<OperationResult<byte[]>> future = syncOperationQueue
						.submit(getOperation);
				futures.add(future);
			} finally {
			}
		}
		try {
			latch.await(config.getGetOperationTimeout(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		// stop waiting if any of the following occur
		// 1) successful response from r nodes
		// 2) response completes successfully or not from n nodes
		// 3) timeout exceeded
		while ((successCounter.get() < config.getRequiredReads())
				&& (resultCollecter.size() < config.getReplicas())
				&& ((System.currentTimeMillis() - start) < config
						.getGetOperationTimeout())) {
			try {
				Future<OperationResult<byte[]>> future = futures.pop();
				if (future != null) {
					try {
						OperationResult<byte[]> result = future.get(config
								.getGetOperationTimeout()
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

		List<Context<byte[]>> results = new ArrayList<Context<byte[]>>(
				resultCollecter.size());
		for (OperationResult<byte[]> result : resultCollecter) {
			Node node = result.getNode();
			int rank = nodeList.indexOf(node);
			byte[] value = result.getValue();
			results.add(new DefaultContext<byte[]>(0, value));
		}
		if (successCounter.get() < config.getRequiredReads()) {
			throw new InsufficientResponsesException(config.getRequiredReads(),
					successCounter.get());
		}
		return results;
	}

	public void set(String key, byte[] object) throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("set(%1$s, %2$s)", key, object));

		long start = System.currentTimeMillis();

		long hashCode = hash.hash(key);

		// ask for a response from x nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(hashCode, config
				.getReplicas());
		LinkedList<Future<OperationResult<byte[]>>> futures = new LinkedList<Future<OperationResult<byte[]>>>();

		// wait for response from r nodes
		CountDownLatch latch = new CountDownLatch(config.getRequiredWrites());
		AtomicInteger successCounter = new AtomicInteger(0);
		List<OperationResult<byte[]>> resultCollecter = new ArrayList<OperationResult<byte[]>>(
				futures.size());
		OperationCallback<byte[]> callback = new OperationCallback<byte[]>() {

			private CountDownLatch latch;

			private AtomicInteger successCounter;

			private List<OperationResult<byte[]>> resultCollecter;

			public OperationCallback<byte[]> init(CountDownLatch latch,
					AtomicInteger successCounter,
					List<OperationResult<byte[]>> resultCollecter) {
				this.latch = latch;
				this.successCounter = successCounter;
				this.resultCollecter = resultCollecter;
				return this;
			}

			public void success(OperationResult<byte[]> result) {
				resultCollecter.add(result);
				latch.countDown();
				successCounter.incrementAndGet();
			}

			public void error(OperationResult<byte[]> result, Exception e) {
				resultCollecter.add(result);
				latch.countDown();
			}
		}.init(latch, successCounter, resultCollecter);
		for (Node node : nodeList) {
			SetOperation<byte[]> setOperation = new SetOperation<byte[]>(
					callback, node, key, object);
			Future<OperationResult<byte[]>> future = syncOperationQueue
					.submit(setOperation);
			futures.add(future);
		}
		try {
			latch.await(config.getGetOperationTimeout(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			log.warn("InterruptedException calling await()", e);
		}

		// stop waiting if any of the following occur
		// 1) successful response from w nodes
		// 2) response completes successfully or not from n nodes
		// 3) timeout exceeded
		while ((successCounter.get() < config.getRequiredWrites())
				&& (resultCollecter.size() < config.getReplicas())
				&& ((System.currentTimeMillis() - start) < config
						.getGetOperationTimeout())) {
			try {
				Future<OperationResult<byte[]>> future = futures.pop();
				if (future != null) {
					try {
						OperationResult<byte[]> result = future.get(config
								.getSetOperationTimeout()
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
		if (successCounter.get() < config.getRequiredWrites())
			throw new InsufficientResponsesException(
					config.getRequiredWrites(), successCounter.get());
	}

	public void set(String key, Context<byte[]> bytes) {
		if (log.isTraceEnabled())
			log.trace(String.format("set(%1$s, %2$s, %3$s)", key, bytes));
		// TODO Auto-generated method stub

	}

	public void delete(String key) throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("delete(%1$s)", key));

		long start = System.currentTimeMillis();

		long hashCode = hash.hash(key);

		// ask for a response from x nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(hashCode, config
				.getReplicas());
		LinkedList<Future<OperationResult<byte[]>>> futures = new LinkedList<Future<OperationResult<byte[]>>>();

		// wait for response from r nodes
		CountDownLatch latch = new CountDownLatch(config.getRequiredWrites());
		AtomicInteger successCounter = new AtomicInteger(0);
		List<OperationResult<byte[]>> resultCollecter = new ArrayList<OperationResult<byte[]>>(
				futures.size());
		OperationCallback<byte[]> callback = new OperationCallback<byte[]>() {

			private CountDownLatch latch;

			private AtomicInteger successCounter;

			private List<OperationResult<byte[]>> resultCollecter;

			public OperationCallback<byte[]> init(CountDownLatch latch,
					AtomicInteger successCounter,
					List<OperationResult<byte[]>> resultCollecter) {
				this.latch = latch;
				this.successCounter = successCounter;
				this.resultCollecter = resultCollecter;
				return this;
			}

			public void success(OperationResult<byte[]> result) {
				resultCollecter.add(result);
				latch.countDown();
				successCounter.incrementAndGet();
			}

			public void error(OperationResult<byte[]> result, Exception e) {
				resultCollecter.add(result);
				latch.countDown();
			}
		}.init(latch, successCounter, resultCollecter);
		for (Node node : nodeList) {
			DeleteOperation<byte[]> op = new DeleteOperation<byte[]>(callback,
					node, key);
			Future<OperationResult<byte[]>> future = syncOperationQueue
					.submit(op);
			futures.add(future);
		}
		try {
			latch.await(config.getGetOperationTimeout(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			log.warn("InterruptedException calling await()", e);
		}

		// stop waiting if any of the following occur
		// 1) successful response from w nodes
		// 2) response completes successfully or not from n nodes
		// 3) timeout exceeded
		while ((successCounter.get() < config.getRequiredWrites())
				&& (resultCollecter.size() < config.getReplicas())
				&& ((System.currentTimeMillis() - start) < config
						.getGetOperationTimeout())) {
			try {
				Future<OperationResult<byte[]>> future = futures.pop();
				if (future != null) {
					try {
						OperationResult<byte[]> result = future.get(config
								.getSetOperationTimeout()
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
		if (successCounter.get() < config.getRequiredWrites())
			throw new InsufficientResponsesException(
					config.getRequiredWrites(), successCounter.get());
	}

	private static class DefaultContext<V> implements Context<V> {
		private int version;

		private V value;

		public DefaultContext(int version, V value) {
			this.version = version;
			this.value = value;
		}

		public int getVersion() {
			return version;
		}

		public V getValue() {
			return value;
		}

		public void setValue(V value) {
			this.value = value;
		}

	}
}
