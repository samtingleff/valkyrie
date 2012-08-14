package com.rubiconproject.oss.kv.distributed.test;

import java.util.ArrayList;
import java.util.List;

import com.rubiconproject.oss.kv.distributed.Configuration;
import com.rubiconproject.oss.kv.distributed.Context;
import com.rubiconproject.oss.kv.distributed.ContextFilter;
import com.rubiconproject.oss.kv.distributed.ContextFilterResult;
import com.rubiconproject.oss.kv.distributed.Node;
import com.rubiconproject.oss.kv.distributed.Operation;
import com.rubiconproject.oss.kv.distributed.OperationStatus;
import com.rubiconproject.oss.kv.distributed.impl.DefaultContext;
import com.rubiconproject.oss.kv.distributed.impl.DefaultNodeImpl;
import com.rubiconproject.oss.kv.distributed.impl.DefaultOperationResult;
import com.rubiconproject.oss.kv.distributed.impl.NodeRankContextFilter;

import junit.framework.TestCase;

public class ContextFilterTestCase extends TestCase {

	public void testNodeRankContextFilter() throws Exception {
		ContextFilter<String> filter = new NodeRankContextFilter<String>(new Configuration());
		List<Context<String>> contexts = new ArrayList<Context<String>>(3);
		for (int i = 2; i >= 0; --i) {
			Node n = new DefaultNodeImpl(i, i, "salt:" + i, "hash://localhost");
			Context<String> ctx = (i != 0) ? new DefaultContext<String>(new DefaultOperationResult<String>(null, "hello world", OperationStatus.Success, 100l, null), n, i,
					0, "test.key", "hello world") : new DefaultContext<String>(
							new DefaultOperationResult<String>(null, "hello world", OperationStatus.NullValue, 200l, null), n, i, 0, "test.key", null);
			contexts.add(ctx);
		}
		ContextFilterResult<String> result = filter.filter(contexts);
		assertNotNull(result);
		Context<String> value = result.getContext();
		assertNotNull(value);
		assertNotNull(value.getValue());
		assertEquals(value.getValue(), "hello world");
		assertEquals(value.getNodeRank(), 1);
		List<Operation<String>> ops = result.getAdditionalOperations();
		assertNotNull(ops);
		assertEquals(ops.size(), 1);
		assertEquals(ops.get(0).getNode().getId(), 0);
	}
}
