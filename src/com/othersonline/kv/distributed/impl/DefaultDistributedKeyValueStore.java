package com.othersonline.kv.distributed.impl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextFilter;
import com.othersonline.kv.distributed.ContextFilterResult;
import com.othersonline.kv.distributed.ContextSerializer;
import com.othersonline.kv.distributed.DistributedKeyValueStore;
import com.othersonline.kv.distributed.HashAlgorithm;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeLocator;
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

	private NodeLocator nodeLocator;

	private OperationQueue syncOperationQueue;

	private OperationQueue asyncOperationQueue;

	private ContextSerializer contextSerializer;

	private ContextFilter<byte[]> contextFilter;

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

	public void setNodeLocator(NodeLocator locator) {
		this.nodeLocator = locator;
	}

	public void setSyncOperationQueue(OperationQueue queue) {
		this.syncOperationQueue = queue;
	}

	public void setAsyncOperationQueue(OperationQueue queue) {
		this.asyncOperationQueue = queue;
	}

	public void setContextSerializer(ContextSerializer contextSerializer) {
		this.contextSerializer = contextSerializer;
	}

	public void setContextFilter(ContextFilter<byte[]> filter) {
		this.contextFilter = filter;
	}

	/**
	 * Low-level method to retrieve all versions for a given key.
	 */
	public List<Context<byte[]>> getContexts(String key)
			throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("getContexts(%1$s)", key));

		// ask for a response from n nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(hash, key, config
				.getReadReplicas());

		Operation<byte[]> op = new GetOperation<byte[]>(transcoder, key);
		List<OperationResult<byte[]>> results = operationHelper.call(
				syncOperationQueue, op, nodeList, config.getRequiredReads(),
				config.getReadOperationTimeout());

		// copy to new list to avoid ConcurrentModificationException
		List<OperationResult<byte[]>> resultCopy = new LinkedList<OperationResult<byte[]>>();
		resultCopy.addAll(results);
		List<Context<byte[]>> retval = new ArrayList<Context<byte[]>>(
				resultCopy.size());
		for (OperationResult<byte[]> result : resultCopy) {
			Node node = result.getNode();
			Context<byte[]> context = contextSerializer.extractContext(node,
					result.getNodeRank(), key, result.getValue());
			retval.add(context);
		}
		return retval;
	}

	public Context<byte[]> get(String key) throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("get(%1$s)", key));
		return get(key, contextFilter);
	}

	public Context<byte[]> get(String key, ContextFilter<byte[]> filter)
			throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("get(%1$s, %2$s)", key, filter));
		List<Context<byte[]>> contexts = getContexts(key);
		ContextFilterResult<byte[]> filtered = filter.filter(contexts);
		List<Operation<byte[]>> additionalOperations = filtered
				.getAdditionalOperations();
		if (additionalOperations != null) {
			for (Operation<byte[]> op : additionalOperations) {
				asyncOperationQueue.submit(op);
			}
		}
		Context<byte[]> result = filtered.getContext();
		return result;
	}

	public void set(String key, byte[] object) throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("set(%1$s, %2$s)", key, object));

		// ask for a response from x nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(hash, key, config
				.getWriteReplicas());

		byte[] serializedData = contextSerializer.addContext(object);
		Operation<byte[]> op = new SetOperation<byte[]>(transcoder, key,
				serializedData);
		List<OperationResult<byte[]>> results = operationHelper.call(
				syncOperationQueue, op, nodeList, config.getRequiredWrites(),
				config.getWriteOperationTimeout());
	}

	public void set(String key, Context<byte[]> bytes) {
		if (log.isTraceEnabled())
			log.trace(String.format("set(%1$s, %2$s, %3$s)", key, bytes));
		// TODO
	}

	public void delete(String key) throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("delete(%1$s)", key));

		// ask for a response from x nodes
		List<Node> nodeList = nodeLocator.getPreferenceList(hash, key, config
				.getWriteReplicas());

		Operation<byte[]> op = new DeleteOperation<byte[]>(key);
		List<OperationResult<byte[]>> results = operationHelper.call(
				syncOperationQueue, op, nodeList, config.getRequiredWrites(),
				config.getWriteOperationTimeout());
	}
}
