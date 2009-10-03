package com.othersonline.kv.distributed.impl;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.distributed.BulkContext;
import com.othersonline.kv.distributed.BulkOperationResult;
import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextFilter;
import com.othersonline.kv.distributed.ContextFilterResult;
import com.othersonline.kv.distributed.ContextSerializer;
import com.othersonline.kv.distributed.DistributedKeyValueStore;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeLocator;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;
import com.othersonline.kv.distributed.hashing.HashAlgorithm;
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

	public Configuration getConfiguration() {
		return config;
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

	public void start() {
		if (contextFilter == null)
			contextFilter = new NodeRankContextFilter<byte[]>(config);
	}

	/**
	 * Low-level method to return node list for a given key.
	 */
	public List<Node> getPreferenceList(String key, int replicas) {
		if (log.isTraceEnabled())
			log.trace(String.format("getPreferenceList(%1$s, %2$d)", key,
					replicas));
		return nodeLocator.getPreferenceList(hash, key, replicas);
	}

	/**
	 * Low-level method to retrieve all versions for a given key.
	 */
	public List<Context<byte[]>> getContexts(String key,
			boolean considerNullAsSuccess, boolean enableSlidingWindow,
			long singleRequestTimeout, long operationTimeout)
			throws KeyValueStoreException {
		if (log.isTraceEnabled())
			log.trace(String.format("getContexts(%1$s)", key));

		long start = System.currentTimeMillis();

		List<Node> nodeList = nodeLocator.getFullPreferenceList(hash, key);

		Operation<byte[]> op = new GetOperation<byte[]>(transcoder, key);

		List<Context<byte[]>> retval = new ArrayList<Context<byte[]>>();

		int offset = 0;

		// While time remaining, ask the next r nodes for a response.
		while ((System.currentTimeMillis() - start) < operationTimeout) {
			if (log.isDebugEnabled()) {
				log.debug("Reaching into node list at offset " + offset
						+ " for " + config.getReadReplicas() + " nodes");
			}

			int toIndex = Math.min(offset + config.getReadReplicas(), nodeList
					.size());
			if (offset > toIndex)
				break;
			List<Node> nodeSublist = nodeList.subList(offset, toIndex);

			if (nodeSublist.size() == 0)
				break;

			// timeout for this request
			// take the smaller of (1) provided single request timeout;
			// or (2) (operation timeout - elapsed time)
			long thisRequestTimeout = Math.min(singleRequestTimeout,
					operationTimeout - (System.currentTimeMillis() - start));

			// ask for results from n nodes with a given offset and timeout
			ResultsCollecter<OperationResult<byte[]>> results = operationHelper
					.call(syncOperationQueue, op, nodeSublist, config
							.getRequiredReads(), thisRequestTimeout,
							considerNullAsSuccess);
			results.stop();
			for (OperationResult<byte[]> result : results) {
				Context<byte[]> context = contextSerializer
						.extractContext(result);
				retval.add(context);
			}
			if ((retval.size() >= config.getRequiredReads())
					|| !enableSlidingWindow)
				break;
			offset += config.getReadReplicas();
		}

		// backfill null/error responses from top x nodes
		ContextFilterResult<byte[]> filtered = contextFilter.filter(retval);
		List<Operation<byte[]>> additionalOperations = filtered
				.getAdditionalOperations();
		if (additionalOperations != null) {
			for (Operation<byte[]> backfillOperation : additionalOperations) {
				asyncOperationQueue.submit(backfillOperation);
			}
		}

		return retval;
	}

	/**
	 * Low-level method to retrieve all versions for a set of keys.
	 */
	public List<BulkContext<byte[]>> getBulkContexts(String... keys)
			throws KeyValueStoreException {

		List<BulkContext<byte[]>> retval;

		Map<Integer, Node> nodes = new HashMap<Integer, Node>();
		List<Node> nodeList = new ArrayList<Node>();

		// generate list of distinct nodes for all keys
		{
			for (String key : keys) {
				List<Node> readNodes = nodeLocator.getPreferenceList(hash, key,
						config.getReadReplicas());
				for (Node node : readNodes) {
					nodes.put(node.getId(), node);
				}
			}
			nodeList.addAll(nodes.values());
		}

		// ask for a response from n nodes
		{
			Operation<byte[]> op;
			ResultsCollecter<OperationResult<byte[]>> results;

			op = new GetBulkOperation<byte[]>(transcoder, keys);
			results = operationHelper.call(syncOperationQueue, op, nodeList,
					config.getRequiredReads(),
					config.getReadOperationTimeout(), true);
			results.stop();

			retval = new ArrayList<BulkContext<byte[]>>(results.size());

			for (OperationResult<byte[]> result : results) {
				BulkContext<byte[]> context = contextSerializer
						.extractBulkContext((BulkOperationResult<byte[]>) result);
				retval.add(context);
			}
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

		// by default, accept null responses as success and disable the sliding
		// window
		List<Context<byte[]>> contexts = getContexts(key, true, false, config
				.getReadOperationTimeout(), config.getReadOperationTimeout());
		ContextFilterResult<byte[]> filtered = filter.filter(contexts);
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
		ResultsCollecter<OperationResult<byte[]>> results = operationHelper
				.call(syncOperationQueue, op, nodeList, config
						.getRequiredWrites(),
						config.getWriteOperationTimeout(), true);
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
		ResultsCollecter<OperationResult<byte[]>> results = operationHelper
				.call(syncOperationQueue, op, nodeList, config
						.getRequiredWrites(),
						config.getWriteOperationTimeout(), true);
	}
}
