package com.othersonline.kv.test.backends;

import java.util.List;

import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.OperationStatus;
import com.othersonline.kv.distributed.impl.DistributedKeyValueStoreClientImpl;
import com.othersonline.kv.distributed.impl.PropertiesConfigurator;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;
import com.othersonline.kv.transcoder.GzippingTranscoder;
import com.othersonline.kv.transcoder.StringTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class ValkyrieBackendTestCase extends KeyValueStoreBackendTestCase {

	public void testBackend() throws Exception {
		PropertiesConfigurator configurator = new PropertiesConfigurator();
		configurator
				.load(getClass()
						.getResourceAsStream(
								"/com/othersonline/kv/test/resources/valkyrie-test.properties"));
		DistributedKeyValueStoreClientImpl store = new DistributedKeyValueStoreClientImpl();
		store.setConfigurator(configurator);
		doTestBackend(store);

		// test getContexts()
		String key = "test.key";
		SampleV v1 = new SampleV(10, "hello world", 12.23);
		store.set(key, v1);
		List<Context<SampleV>> contexts = store.getContexts(key);
		assertNotNull(contexts);
		assertEquals(contexts.size(), 1);
		SampleV v2 = contexts.get(0).getValue();
		assertEquals(contexts.get(0).getResult().getStatus(),
				OperationStatus.Success);
		assertTrue(v1.equals(v2));
		assertEquals(v1.compareTo(v2), 0);

		// test getContexts with a transcoder
		Transcoder transcoder = new GzippingTranscoder(new StringTranscoder());
		store.set(key, "short string", transcoder);
		List<Context<String>> stringContexts = store.getContexts(key,
				transcoder);
		assertNotNull(stringContexts);
		assertEquals(stringContexts.size(), 1);
		String value = stringContexts.get(0).getValue();
		assertEquals(contexts.get(0).getResult().getStatus(),
				OperationStatus.Success);
		assertEquals(value, "short string");

		// clean up
		store.delete(key);
		stringContexts = store.getContexts(key, transcoder);
		assertNotNull(stringContexts);
		assertEquals(stringContexts.size(), 1);
		assertEquals(stringContexts.get(0).getResult().getStatus(),
				OperationStatus.NullValue);
	}

}
