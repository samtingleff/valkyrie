package com.othersonline.kv.distributed.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.DistributedKeyValueStoreClient;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.backends.ConnectionFactory;
import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.ConfigurationException;
import com.othersonline.kv.distributed.Configurator;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.hashing.MD5HashAlgorithm;
import com.othersonline.kv.transcoder.SerializingTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class DistributedKeyValueStoreClientImpl extends
		BaseManagedKeyValueStore implements KeyValueStore,
		DistributedKeyValueStoreClient {
	public static final String IDENTIFIER = "yahbadc";

	private static OperationLog log = OperationLog.getInstance();

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
			store.setContextFilter(new NodeRankContextFilter<byte[]>(config));
			store.start();
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
		long start = System.currentTimeMillis();
		boolean success = true;
		try {
			assertReadable();
			Object obj = get(key);
			return (obj != null);
		} catch (KeyValueStoreException e1) {
			success = false;
			throw e1;
		} catch (IOException e2) {
			success = false;
			throw e2;
		} finally {
			log(key, "exists", System.currentTimeMillis() - start, success);
		}
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		long start = System.currentTimeMillis();
		boolean success = true;
		try {
			assertReadable();
			Context<byte[]> context = store.get(key);
			byte[] bytes = context.getValue();
			Object obj = null;
			if (bytes != null) {
				obj = transcoder.decode(bytes);
			}
			return obj;
		} catch (KeyValueStoreException e1) {
			success = false;
			throw e1;
		} catch (IOException e2) {
			success = false;
			throw e2;
		} finally {
			log(key, "get", System.currentTimeMillis() - start, success);
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		long start = System.currentTimeMillis();
		boolean success = true;
		try {
			assertReadable();
			Map<String, Object> map = new HashMap<String, Object>();
			for (String key : keys) {
				Object obj = get(key);
				if (obj != null)
					map.put(key, obj);
			}
			return map;
		} catch (KeyValueStoreException e1) {
			success = false;
			throw e1;
		} catch (IOException e2) {
			success = false;
			throw e2;
		} finally {
			log("null", "getbulk", System.currentTimeMillis() - start, success);
		}
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		long start = System.currentTimeMillis();
		boolean success = true;
		try {
			assertReadable();
			Map<String, Object> map = new HashMap<String, Object>();
			for (String key : keys) {
				Object obj = get(key);
				if (obj != null)
					map.put(key, obj);
			}
			return map;
		} catch (KeyValueStoreException e1) {
			success = false;
			throw e1;
		} catch (IOException e2) {
			success = false;
			throw e2;
		} finally {
			log("null", "getbulk", System.currentTimeMillis() - start, success);
		}
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		long start = System.currentTimeMillis();
		boolean success = true;
		try {
			assertReadable();
			Map<String, Object> map = new HashMap<String, Object>();
			for (String key : keys) {
				Object obj = get(key, transcoder);
				if (obj != null)
					map.put(key, obj);
			}
			return map;
		} catch (KeyValueStoreException e1) {
			success = false;
			throw e1;
		} catch (IOException e2) {
			success = false;
			throw e2;
		} finally {
			log("null", "getbulk", System.currentTimeMillis() - start, success);
		}
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		long start = System.currentTimeMillis();
		boolean success = true;
		try {
			assertWriteable();
			byte[] bytes = transcoder.encode(value);
			store.set(key, bytes);
		} catch (KeyValueStoreException e1) {
			success = false;
			throw e1;
		} catch (IOException e2) {
			success = false;
			throw e2;
		} finally {
			log(key, "set", System.currentTimeMillis() - start, success);
		}
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		long start = System.currentTimeMillis();
		boolean success = true;
		try {
			assertWriteable();
			store.delete(key);
		} catch (KeyValueStoreException e1) {
			success = false;
			throw e1;
		} finally {
			log(key, "delete", System.currentTimeMillis() - start, success);
		}
	}

	private void log(String key, String op, long duration, boolean success) {
		log.log(key, op, duration, success);
	}

}
