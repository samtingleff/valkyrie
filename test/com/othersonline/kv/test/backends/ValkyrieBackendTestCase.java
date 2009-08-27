package com.othersonline.kv.test.backends;

import com.othersonline.kv.distributed.impl.DistributedKeyValueStoreClientImpl;
import com.othersonline.kv.distributed.impl.PropertiesConfigurator;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

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
	}

}
