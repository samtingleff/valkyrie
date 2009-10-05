package com.othersonline.kv.distributed.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextFilter;
import com.othersonline.kv.distributed.ContextFilterResult;
import com.othersonline.kv.distributed.DistributedKeyValueStoreException;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationStatus;

/**
 * A simple context filter that orders results by node rank from the preference
 * list. Other nodes that responde with a null value or an exception will be
 * updated with the selected value (if configured to do so).
 * 
 * @author Sam Tingleff <sam@tingleff.com>
 * 
 * @param <V>
 */
public class NodeRankContextFilter<V> implements ContextFilter<V> {

	private Log log = LogFactory.getLog(getClass());

	private Log backfillLog = LogFactory.getLog("haymitch.backfilllog");

	private Comparator<Context<V>> comparator = new NodeRankComparator<V>();

	private Configuration config;

	public NodeRankContextFilter(Configuration config) {
		this.config = config;
	}

	public ContextFilterResult<V> filter(List<Context<V>> contexts)
			throws DistributedKeyValueStoreException {
		Collections.sort(contexts, comparator);

		// Select the first non-null value and call set() on any null nodes.
		Context<V> lowestNonNullValueContext = null;

		List<Node> nodesRequiringUpdate = new LinkedList<Node>();
		for (Context<V> context : contexts) {
			if (lowestNonNullValueContext == null)
				lowestNonNullValueContext = context;
			if ((lowestNonNullValueContext.getValue() == null)
					&& (context.getValue() != null)) {
				lowestNonNullValueContext = context;
			}
		}
		if (log.isDebugEnabled() && (lowestNonNullValueContext != null))
			log.debug(String.format("Selected context from node # %1$d",
					lowestNonNullValueContext.getSourceNode().getId()));
		if ((lowestNonNullValueContext != null)
				&& (lowestNonNullValueContext.getValue() != null)) {
			for (Context<V> context : contexts) {
				if (context.getValue() == null) {
					OperationStatus status = context.getResult().getStatus();
					if ((status.equals(OperationStatus.NullValue))
							&& (config.getFillNullGetResults())
							&& (context.getNodeRank() < config.getWriteReplicas())) {
						if (log.isDebugEnabled())
							log
									.debug(String
											.format(
													"Node # %1$d requires update due to null value",
													context.getSourceNode()
															.getId()));
						nodesRequiringUpdate.add(context.getSourceNode());
					} else if ((OperationStatus.Error.equals(status))
							&& (config.getFillErrorGetResults())) {
						if (log.isDebugEnabled())
							log.debug(String.format(
									"Node # %1$d requires update due to error",
									context.getSourceNode().getId()));
						nodesRequiringUpdate.add(context.getSourceNode());
					}
				}
			}
		}

		List<Operation<V>> ops = null;
		if ((lowestNonNullValueContext != null)
				&& (nodesRequiringUpdate.size() > 0)) {
			ops = new LinkedList<Operation<V>>();
			for (Node node : nodesRequiringUpdate) {
				Operation<V> op = new SetOperation<V>(null,
						lowestNonNullValueContext.getKey(),
						lowestNonNullValueContext.getValue());
				op.setNode(node);
				ops.add(op);
			}
		}
		log(lowestNonNullValueContext, ops);
		return new DefaultContextFilterResult<V>(lowestNonNullValueContext, ops);
	}

	private void log(Context<V> choice, List<Operation<V>> ops) {
		if (backfillLog.isInfoEnabled() && (choice != null) && (ops != null)) {
			StringBuffer format = new StringBuffer();
			for (int i = 0; i < ops.size(); ++i) {
				if (i > 0)
					format.append(',');
				format.append(ops.get(i).getNode().getId());
			}
			backfillLog.info(String.format("%1$s\t%2$s", choice.getSourceNode()
					.getId(), format.toString()));
		}
	}

	private static class NodeRankComparator<V> implements
			Comparator<Context<V>> {
		public int compare(Context<V> o1, Context<V> o2) {
			return new Integer(o1.getNodeRank()).compareTo(new Integer(o2
					.getNodeRank()));
		}
	}
}
