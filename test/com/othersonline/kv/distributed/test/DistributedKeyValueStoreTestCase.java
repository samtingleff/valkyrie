package com.othersonline.kv.distributed.test;

import java.util.List;

import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.DefaultDistributedKeyValueStore;
import com.othersonline.kv.distributed.backends.TokyoTyrantConnectionFactory;

import junit.framework.TestCase;

public class DistributedKeyValueStoreTestCase extends TestCase {

	public void testSimpleDistributedKeyValueStore() throws Exception {
		Configuration config = new Configuration();
		config.setRequiredReads(1);
		config.setRequiredWrites(1);
		config.setReplicas(1);
		ConnectionFactory cf = new TokyoTyrantConnectionFactory();
		DefaultDistributedKeyValueStore kv = new DefaultDistributedKeyValueStore();
		kv.setAsyncOperationQueue(new DummyOperationQueue(cf));
		kv.setConfiguration(config);
		kv.setConnectionFactory(cf);
		kv.setContextSerializer(null);
		kv.setHashAlgorithm(new HashCodeHashAlgorithm());
		kv.setNodeLocator(new DummyNodeLocator());
		kv.setSyncOperationQueue(new DummyOperationQueue(cf));

		String key = "test.key";
		String value = "hello world 2";
		kv.set(key, value.getBytes());

		List<Context<byte[]>> values = kv.get(key);
		assertEquals(values.size(), 1);
		Context<byte[]> context = values.get(0);
		String s = new String(context.getValue());
		assertEquals(s, value);

		kv.delete(key);

		values = kv.get(key);
		assertEquals(values.size(), 1);
		context = values.get(0);
		assertNull(context.getValue());
	}
}
