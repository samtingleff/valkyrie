package com.othersonline.kv.distributed.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.DistributedKeyValueStoreClient;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.ConfigurationException;
import com.othersonline.kv.distributed.Configurator;
import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.hashing.MD5HashAlgorithm;
import com.othersonline.kv.transcoder.SerializingTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class DistributedKeyValueStoreClientImpl extends
		BaseManagedKeyValueStore implements KeyValueStore,
		DistributedKeyValueStoreClient {
	public static final String IDENTIFIER = "yahbadc";

	private Configurator configurator;

	private DefaultDistributedKeyValueStore store;

	private Transcoder defaultTranscoder = new SerializingTranscoder();

	public DistributedKeyValueStoreClientImpl() {
	}

	public DistributedKeyValueStoreClientImpl(Configurator configurator)
			throws IOException {
		this.configurator = configurator;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void setConfigurator(Configurator configurator) {
		this.configurator = configurator;
	}

	@Override
	public void start() throws IOException {
		try {
			Configuration config = configurator.getConfiguration();
			ConnectionFactory connectionFactory = config.getConnectionFactory();
			OperationQueue syncOpQueue = config.getSyncOperationQueue();
			syncOpQueue.setConnectionFactory(connectionFactory);
			syncOpQueue.start();
			OperationQueue asyncOpQueue = config.getAsyncOperationQueue();
			asyncOpQueue.setConnectionFactory(connectionFactory);
			asyncOpQueue.start();

			DynamoNodeLocator nl = new DynamoNodeLocator();
			config.getNodeStore().addChangeListener(nl);
			config.getNodeStore().start();
			store = new DefaultDistributedKeyValueStore();
			store.setConfiguration(config);
			store.setHashAlgorithm(new MD5HashAlgorithm());
			store.setNodeLocator(nl);
			store.setSyncOperationQueue(syncOpQueue);
			store.setAsyncOperationQueue(asyncOpQueue);
			store.setContextSerializer(new PassthroughContextSerializer());
			store.setContextFilter(new NodeRankContextFilter<byte[]>());
			super.start();
		} catch (ConfigurationException e) {
			throw new IOException(e);
		} finally {

		}
	}

	@Override
	public void stop() {
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		Object obj = get(key);
		return (obj != null);
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Context<byte[]> context = store.get(key);
		byte[] bytes = context.getValue();
		Object obj = null;
		if (bytes != null) {
			obj = transcoder.decode(bytes);
		}
		return obj;
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> map = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				map.put(key, obj);
		}
		return map;
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> map = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				map.put(key, obj);
		}
		return map;
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> map = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key, transcoder);
			if (obj != null)
				map.put(key, obj);
		}
		return map;
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		byte[] bytes = transcoder.encode(value);
		store.set(key, bytes);
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		store.delete(key);
	}

}
