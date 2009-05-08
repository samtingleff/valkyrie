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

	private NodeStore nodeStore;

	private NodeLocator nodeLocator;

	private OperationQueue syncOperationQueue;

	private OperationQueue asyncOperationQueue;

	private ConnectionFactory connectionFactory;

	private ContextSerializer contextSerializer;

	private DefaultOperationHelper operationHelper = new DefaultOperationHelper();

	public DefaultDistributedKeyValueStore() {
	}

	public void setConfiguration(Configuration config) {
		this.config = config;
	}

	public void setHashAlgorithm(HashAlgorithm hash) {
		this.hash = hash;
	}

	public void setNodeStore(NodeStore nodeStore) {
		this.nodeStore = nodeStore;
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
		;

		// ask for a response from n nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(key, nodeStore
				.getActiveNodes(), config.getReplicas());

		Operation<byte[]> op = new GetOperation<byte[]>(key);
		List<OperationResult<byte[]>> results = operationHelper.call(
				syncOperationQueue, op, nodeList, config.getRequiredReads(),
				config.getGetOperationTimeout());

		List<Context<byte[]>> retval = new ArrayList<Context<byte[]>>(results
				.size());
		for (OperationResult<byte[]> result : results) {
			Node node = result.getNode();
			int rank = nodeList.indexOf(node);
			byte[] value = result.getValue();
			retval.add(new DefaultContext<byte[]>(0, value));
		}
		return retval;
	}

	public void set(String key, byte[] object) throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("set(%1$s, %2$s)", key, object));

		// ask for a response from x nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(key, nodeStore
				.getActiveNodes(), config.getReplicas());

		Operation<byte[]> op = new SetOperation<byte[]>(key, object);
		List<OperationResult<byte[]>> results = operationHelper.call(
				syncOperationQueue, op, nodeList, config.getRequiredWrites(),
				config.getSetOperationTimeout());
	}

	public void set(String key, Context<byte[]> bytes) {
		if (log.isTraceEnabled())
			log.trace(String.format("set(%1$s, %2$s, %3$s)", key, bytes));
		// TODO Auto-generated method stub

	}

	public void delete(String key) throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("delete(%1$s)", key));

		// ask for a response from x nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(key, nodeStore
				.getActiveNodes(), config.getReplicas());

		Operation<byte[]> op = new DeleteOperation<byte[]>(key);
		List<OperationResult<byte[]>> results = operationHelper.call(
				syncOperationQueue, op, nodeList, config.getRequiredWrites(),
				config.getSetOperationTimeout());
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
