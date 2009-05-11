package com.othersonline.kv.distributed.test;

import java.util.ArrayList;
import java.util.List;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.ContextFilter;
import com.othersonline.kv.distributed.ContextFilterResult;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.impl.DefaultContext;
import com.othersonline.kv.distributed.impl.DefaultNodeImpl;
import com.othersonline.kv.distributed.impl.NodeRankContextFilter;

import junit.framework.TestCase;

public class ContextFilterTestCase extends TestCase {

	public void testNodeRankContextFilter() throws Exception {
		ContextFilter<String> filter = new NodeRankContextFilter<String>();
		List<Context<String>> contexts = new ArrayList<Context<String>>(3);
		for (int i = 2; i >= 0; --i) {
			Node n = new DefaultNodeImpl(i, i, "salt:" + i, "hash://localhost",
					null);
			Context<String> ctx = (i != 0) ? new DefaultContext<String>(n, i,
					0, "test.key", "hello world") : new DefaultContext<String>(
					n, i, 0, "test.key", null);
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
		Operation<String> op = ops.get(0);
		assertEquals(op.getNodeRank(), 0);
	}
}
