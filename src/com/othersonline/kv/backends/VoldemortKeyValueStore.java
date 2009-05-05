package com.othersonline.kv.backends;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.versioning.Versioned;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.util.DaemonThreadFactory;
import com.othersonline.kv.util.ExecutorUtils;

public class VoldemortKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "voldemort";

	private int threadPoolSize = 5;

	private int maxConnectionsPerNode = SocketStoreClientFactory.DEFAULT_MAX_CONNECTIONS_PER_NODE;

	private int maxTotalConnections = SocketStoreClientFactory.DEFAULT_MAX_CONNECTIONS;

	private int socketTimeout = SocketStoreClientFactory.DEFAULT_SOCKET_TIMEOUT_MS;

	private int routingTimeout = SocketStoreClientFactory.DEFAULT_ROUTING_TIMEOUT_MS;

	// how long nodes are banned after a socket timeout
	private int nodeBannage = SocketStoreClientFactory.DEFAULT_NODE_BANNAGE_MS;

	private String bootstrapUrl = "tcp://localhost:6666";

	private String storeName = "test";

	private boolean iOwnThreadPool = true;

	private ExecutorService executor;

	private StoreClient<String, Object> client;

	public void setExecutorService(ExecutorService executor) {
		this.executor = executor;
	}

	public void setThreadPoolSize(int threadPoolSize) {
		this.threadPoolSize = threadPoolSize;
	}

	public void setMaxConnectionsPerNode(int maxConnectionsPerNode) {
		this.maxConnectionsPerNode = maxConnectionsPerNode;
	}

	public void setMaxTotalConnections(int maxTotalConnections) {
		this.maxTotalConnections = maxTotalConnections;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public void setRoutingTimeout(int routingTimeout) {
		this.routingTimeout = routingTimeout;
	}

	public void setNodeBannage(int nodeBannage) {
		this.nodeBannage = nodeBannage;
	}

	public void setBootstrapUrl(String bootstrapUrl) {
		this.bootstrapUrl = bootstrapUrl;
	}

	public void setStoreName(String storeName) {
		this.storeName = storeName;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		if (executor == null) {
			executor = Executors.newFixedThreadPool(threadPoolSize,
					new DaemonThreadFactory());
			iOwnThreadPool = true;
		} else
			iOwnThreadPool = false;
		StoreClientFactory factory = new SocketStoreClientFactory(executor,
				maxConnectionsPerNode, maxTotalConnections, socketTimeout,
				routingTimeout, nodeBannage, new DefaultSerializerFactory(),
				bootstrapUrl);
		client = factory.getStoreClient(storeName);
		super.start();
	}

	public void stop() {
		if (iOwnThreadPool) {
			ExecutorUtils.shutdown(executor, TimeUnit.SECONDS, 2l,
					TimeUnit.SECONDS, 2);
			executor = null;
		}
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		Versioned v = client.get(key);
		return (v != null);
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		Object obj = client.getValue(key);
		return obj;
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		byte[] bytes = (byte[]) client.getValue(key);
		Object obj = null;
		if (bytes != null) {
			obj = transcoder.decode(bytes);
		}
		return obj;
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = client.getValue(key);
			if (obj != null) {
				results.put(key, obj);
			}
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = client.getValue(key);
			if (obj != null) {
				results.put(key, obj);
			}
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			byte[] bytes = (byte[]) client.getValue(key);
			if (bytes != null) {
				Object obj = transcoder.decode(bytes);
				results.put(key, obj);
			}
		}
		return results;
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		client.put(key, value);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		byte[] bytes = transcoder.encode(value);
		client.put(key, bytes);
		/*
		 * Versioned version = client.get(key); if (version == null) { version =
		 * new Versioned(bytes); } else { version.setObject(bytes); }
		 * client.put("key", version);
		 */
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		client.delete(key);
	}

	private static class VoldemortSerializerFactory implements
			SerializerFactory {
		private Serializer<byte[]> byteSerializer = new VoldemortByteArraySerializer();

		private Serializer<String> stringSerializer = new VoldemortStringSerializer();

		public Serializer<?> getSerializer(SerializerDefinition def) {
			String name = def.getName();
			if ("bytes".equals(name)) {
				return byteSerializer;
			} else if ("string".equals(name)) {
				return stringSerializer;
			} else {
				return new VoldemortSerializerWrapper(
						new SerializableTranscoder());
			}
		}
	}

	private static class VoldemortByteArraySerializer implements
			Serializer<byte[]> {
		public byte[] toBytes(byte[] bytes) {
			return bytes;
		}

		public byte[] toObject(byte[] bytes) {
			return bytes;
		}
	}

	private static class VoldemortStringSerializer implements
			Serializer<String> {

		public byte[] toBytes(String s) {
			return s.getBytes();
		}

		public String toObject(byte[] bytes) {
			return new String(bytes);
		}

	}

	private static class VoldemortSerializerWrapper<T> implements Serializer<T> {

		private Transcoder transcoder;

		public VoldemortSerializerWrapper(Transcoder transcoder) {
			this.transcoder = transcoder;
		}

		public byte[] toBytes(T obj) {
			try {
				return transcoder.encode(obj);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public T toObject(byte[] bytes) {
			try {
				return (T) transcoder.decode(bytes);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

	}
}
