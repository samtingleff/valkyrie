package com.othersonline.kv.distributed.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextFilter;
import com.othersonline.kv.distributed.ContextFilterResult;
import com.othersonline.kv.distributed.DistributedKeyValueStoreException;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;

/**
 * A simple context filter that orders results by node rank from the preference
 * list. Other nodes that provide a null value will be updated with this value.
 * 
 * @author Sam Tingleff <sam@tingleff.com>
 * 
 * @param <V>
 */
public class NodeRankContextFilter<V> implements ContextFilter<V> {
	private Comparator<Context<V>> comparator = new MyComparator<V>();

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
			if ((lowestNonNullValueContext.getValue() != null)
					&& (context.getValue() == null)) {
				nodesRequiringUpdate.add(context.getSourceNode());
			}
		}

		List<Operation<V>> ops = new LinkedList<Operation<V>>();
		for (Node node : nodesRequiringUpdate) {
			Operation<V> op = new SetOperation<V>(null,
					lowestNonNullValueContext.getKey(),
					lowestNonNullValueContext.getValue());
			op.setNode(node);
			ops.add(op);

		}
		return new DefaultContextFilterResult<V>(lowestNonNullValueContext, ops);
	}

	private static class MyComparator<V> implements Comparator<Context<V>> {
		public int compare(Context<V> o1, Context<V> o2) {
			return new Integer(o1.getNodeRank()).compareTo(new Integer(o2
					.getNodeRank()));
		}
	}
}
