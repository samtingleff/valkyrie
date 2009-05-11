package com.othersonline.kv.distributed.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextFilter;
import com.othersonline.kv.distributed.ContextFilterResult;
import com.othersonline.kv.distributed.DistributedKeyValueStoreException;
import com.othersonline.kv.distributed.Operation;

public class NodeRankContextFilter<V> implements ContextFilter<V> {

	public ContextFilterResult<V> filter(List<Context<V>> contexts)
			throws DistributedKeyValueStoreException {
		Collections.sort(contexts, new Comparator<Context<V>>() {
			public int compare(Context<V> o1, Context<V> o2) {
				return new Integer(o1.getNodeRank()).compareTo(new Integer(o2
						.getNodeRank()));
			}
		});
		// Select the first non-null value and call set() on any nodes higher in
		// the list.
		Context<V> lowestNonNullContext = null;
		List<Operation<V>> ops = new LinkedList<Operation<V>>();
		for (Context<V> context : contexts) {
			if (lowestNonNullContext == null)
				lowestNonNullContext = context;
			if ((lowestNonNullContext.getValue() == null)
					&& (context.getValue() != null)) {
				lowestNonNullContext = context;
				ops.add(new SetOperation<V>(null, context.getKey(), context
						.getValue()));
			}
		}
		return new DefaultContextFilterResult<V>(lowestNonNullContext, ops);
	}
}
