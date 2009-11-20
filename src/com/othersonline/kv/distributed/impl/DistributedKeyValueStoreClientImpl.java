package com.othersonline.kv.distributed.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.DistributedKeyValueStoreClient;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.backends.ConnectionFactory;
import com.othersonline.kv.distributed.BulkContext;
import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.ConfigurationException;
import com.othersonline.kv.distributed.Configurator;
import com.othersonline.kv.distributed.Context;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.Operation;
import com.othersonline.kv.distributed.OperationQueue;
import com.othersonline.kv.distributed.OperationResult;
import com.othersonline.kv.distributed.hashing.MD5HashAlgorithm;
import com.othersonline.kv.transcoder.SerializingTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class DistributedKeyValueStoreClientImpl extends
		BaseManagedKeyValueStore implements KeyValueStore,
		DistributedKeyValueStoreClient {
	public static final String IDENTIFIER = "yahbadc";

	private static OperationLog log = OperationLog.getInstance();

	private Configurator configurator;

	private Configuration config;

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
			this.config = configurator.getConfiguration();
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
		store.stop();
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

	public List<Node> getPreferenceList(String key, int replicas) {
		return store.getPreferenceList(key, replicas);
	}

	public <V> List<Context<V>> getContexts(String key,
			boolean considerNullAsSuccess, boolean enableSlidingWindow,
			long singleRequestTimeout, long operationTimeout)
			throws KeyValueStoreException, IOException {
		return getContexts(key, defaultTranscoder, considerNullAsSuccess,
				enableSlidingWindow, singleRequestTimeout, operationTimeout);
	}

	public <V> List<Context<V>> getContexts(String key, Transcoder transcoder,
			boolean considerNullAsSuccess, boolean enableSlidingWindow,
			long singleRequestTimeout, long operationTimeout)
			throws KeyValueStoreException, IOException {
		long start = System.currentTimeMillis();
		boolean success = true;
		try {
			assertReadable();
			List<Context<byte[]>> contexts = store.getContexts(key,
					considerNullAsSuccess, enableSlidingWindow,
					singleRequestTimeout, operationTimeout);
			List<Context<V>> results = new ArrayList<Context<V>>(contexts
					.size());
			for (Context<byte[]> context : contexts) {
				// check for context value NOT being instanceof byte
				Object value = context.getValue();
				// some backends deliver a V directly rather than byte[]
				V v = (value instanceof byte[]) ? (V) transcoder.decode((byte[]) value) : (V) value;
				Operation<V> op = new GetOperation<V>(transcoder, key);
				OperationResult<V> operationResult = new DefaultOperationResult<V>(
						op, v, context.getResult().getStatus(), context
								.getResult().getDuration(), context.getResult()
								.getError());
				Context<V> ctx = new DefaultContext<V>(operationResult, context
						.getSourceNode(), context.getNodeRank(), context
						.getVersion(), context.getKey(), v);
				results.add(ctx);
			}
			return results;
		} finally {
			log(key, "getContexts", System.currentTimeMillis() - start, success);
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		return getBulk(defaultTranscoder, keys);
	}

	public Map<String, Object> getBulk(Transcoder transcoder, String... keys)
			throws KeyValueStoreException, IOException {
		long start = System.currentTimeMillis();
		boolean success = true;
		try {
			assertReadable();
			Map<String, Object> map = new HashMap<String, Object>();
			List<BulkContext<byte[]>> contexts;

			contexts = store.getBulkContexts(keys);
			for (BulkContext<byte[]> context : contexts) {
				Map<String, byte[]> values = context.getValues();

				for (Map.Entry<String, byte[]> entry : values.entrySet()) {
					Object obj;
					byte[] bytes = entry.getValue();
					obj = transcoder.decode(bytes);

					// TODO deal with multiple values for same key
					if (map.containsKey(entry.getKey()))
						map.put(entry.getKey(), obj);
					else
						map.put(entry.getKey(), obj);
				}
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

		// doing a cast of keys.toArray() to (String[]) causes a
		// ClassCastException
		String[] strings = new String[keys.size()];
		for (int i = 0; i < keys.size(); i++) {
			strings[i] = keys.get(i);
		}

		return getBulk(defaultTranscoder, strings);
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		return getBulk(transcoder, (String[]) keys.toArray());
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

	public Configuration getConfiguration() {
		return config;
	}

	private void log(String key, String op, long duration, boolean success) {
		log.log(key, op, duration, success);
	}

}
