package com.othersonline.kv.distributed.impl;

import java.util.List;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextFilterResult;
import com.othersonline.kv.distributed.Operation;

public class DefaultContextFilterResult<V> implements ContextFilterResult<V> {

	private Context<V> context;

	private List<Operation<V>> additionalOperations;

	public DefaultContextFilterResult(Context<V> context,
			List<Operation<V>> additionalOperations) {
		this.context = context;
		this.additionalOperations = additionalOperations;
	}

	public Context<V> getContext() {
		return context;
	}

	public List<Operation<V>> getAdditionalOperations() {
		return additionalOperations;
	}

}
