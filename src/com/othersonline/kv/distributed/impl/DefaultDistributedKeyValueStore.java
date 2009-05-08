package com.othersonline.kv.distributed.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextSerializer;
import com.othersonline.kv.distributed.DistributedKeyValueStore;
import com.othersonline.kv.distributed.ExtractedContext;
import com.othersonline.kv.distributed.HashAlgorithm;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeLocator;
import com.othersonline.kv.distributed.NodeStore;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;
import com.othersonline.kv.transcoder.ByteArrayTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

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

	private Transcoder transcoder = new ByteArrayTranscoder();

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

		// ask for a response from n nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(hash, key, config
				.getReplicas());

		Operation<byte[]> op = new GetOperation<byte[]>(transcoder, key);
		List<OperationResult<byte[]>> results = operationHelper.call(
				syncOperationQueue, op, nodeList, config.getRequiredReads(),
				config.getReadOperationTimeout());

		List<Context<byte[]>> retval = new ArrayList<Context<byte[]>>(results
				.size());
		for (OperationResult<byte[]> result : results) {
			Node node = result.getNode();
			ExtractedContext<byte[]> ec = contextSerializer.extractContext(
					node, result.getValue());
			if (ec.getAdditionalOperations() != null) {
				for (Operation<byte[]> work : ec.getAdditionalOperations()) {
					asyncOperationQueue.submit(work);
				}
			}
			retval.add(ec.getContext());
		}
		return retval;
	}

	public void set(String key, byte[] object) throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("set(%1$s, %2$s)", key, object));

		// ask for a response from x nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(hash, key, config
				.getReplicas());

		byte[] serializedData = contextSerializer.addContext(object);
		Operation<byte[]> op = new SetOperation<byte[]>(transcoder, key, serializedData);
		List<OperationResult<byte[]>> results = operationHelper.call(
				syncOperationQueue, op, nodeList, config.getRequiredWrites(),
				config.getWriteOperationTimeout());
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
		List<Node> nodeList = nodeLocator.getPreferenceList(hash, key, config
				.getReplicas());

		Operation<byte[]> op = new DeleteOperation<byte[]>(key);
		List<OperationResult<byte[]>> results = operationHelper.call(
				syncOperationQueue, op, nodeList, config.getRequiredWrites(),
				config.getWriteOperationTimeout());
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
