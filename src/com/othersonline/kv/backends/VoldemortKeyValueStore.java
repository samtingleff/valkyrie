package com.othersonline.kv.backends;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.StringSerializer;
import voldemort.versioning.Versioned;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.transcoder.SerializingTranscoder;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.util.DaemonThreadFactory;
import com.othersonline.kv.util.ExecutorUtils;

public class VoldemortKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "voldemort";

	private Transcoder defaultTranscoder = new SerializingTranscoder();

	private int threadPoolSize = 5;

	private int maxConnectionsPerNode = 6;

	private int maxTotalConnections = 500;

	private int socketTimeout = 5000;

	private int routingTimeout = 15000;

	// how long nodes are banned after a socket timeout
	private int nodeBannage = 30000;

	private String host = null;

	private int port = 6666;

	private String bootstrapUrl = "tcp://localhost:6666";

	private String storeName = "test";

	private boolean iOwnThreadPool = true;

	private ExecutorService executor;

	private StoreClient<String, Object> client;

	public VoldemortKeyValueStore(String bootstrapUrl) {
		this.bootstrapUrl = bootstrapUrl;
	}

	public VoldemortKeyValueStore() {
	}

	public void setExecutorService(ExecutorService executor) {
		this.executor = executor;
	}

	@Configurable(name = "host", accepts = Type.StringType)
	public void setHost(String host) {
		this.host = host;
	}

	@Configurable(name = "port", accepts = Type.IntType)
	public void setPort(int port) {
		this.port = port;
	}

	@Configurable(name = "threadPoolSize", accepts = Type.IntType)
	public void setThreadPoolSize(int threadPoolSize) {
		this.threadPoolSize = threadPoolSize;
	}

	@Configurable(name = "maxConnectionsPerNode", accepts = Type.IntType)
	public void setMaxConnectionsPerNode(int maxConnectionsPerNode) {
		this.maxConnectionsPerNode = maxConnectionsPerNode;
	}

	@Configurable(name = "maxTotalConnections", accepts = Type.IntType)
	public void setMaxTotalConnections(int maxTotalConnections) {
		this.maxTotalConnections = maxTotalConnections;
	}

	@Configurable(name = "socketTimeout", accepts = Type.IntType)
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	@Configurable(name = "routingTimeout", accepts = Type.IntType)
	public void setRoutingTimeout(int routingTimeout) {
		this.routingTimeout = routingTimeout;
	}

	@Configurable(name = "nodeBannage", accepts = Type.IntType)
	public void setNodeBannage(int nodeBannage) {
		this.nodeBannage = nodeBannage;
	}

	@Configurable(name = "bootstrapUrl", accepts = Type.StringType)
	public void setBootstrapUrl(String bootstrapUrl) {
		this.bootstrapUrl = bootstrapUrl;
	}

	@Configurable(name = "storeName", accepts = Type.StringType)
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
		String url = (host == null) ? bootstrapUrl : String.format(
				"tcp://%1$s:%2$d", host, port);

		ClientConfig config = new ClientConfig();
		config.setBootstrapUrls(url);
		config.setConnectionTimeout(socketTimeout, TimeUnit.MILLISECONDS);
		config.setSocketTimeout(socketTimeout, TimeUnit.MILLISECONDS);
		config.setRoutingTimeout(routingTimeout, TimeUnit.MILLISECONDS);
		config.setMaxConnectionsPerNode(maxConnectionsPerNode);
		config.setMaxTotalConnections(maxTotalConnections);
		config.setFailureDetectorBannagePeriod(nodeBannage);
		config.setSerializerFactory(new ValkyrieVoldemortSerializerFactory());
		StoreClientFactory factory = new SocketStoreClientFactory(config);
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
		return get(key, defaultTranscoder);
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
				Object decoded = defaultTranscoder.decode((byte[]) obj);
				results.put(key, decoded);
			}
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return getBulk(keys, defaultTranscoder);
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
		set(key, value, defaultTranscoder);
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

	public class ValkyrieVoldemortSerializerFactory implements
			SerializerFactory {

		public Serializer<?> getSerializer(SerializerDefinition def) {
			return new ValkyrieSerializer();
		}
	}

	private static class ValkyrieSerializer implements Serializer {
		private Serializer<String> keySerializer = new StringSerializer();

		public byte[] toBytes(Object obj) {
			if (obj instanceof byte[])
				return (byte[]) obj;
			else {
				return keySerializer.toBytes((String) obj);
			}
		}

		public Object toObject(byte[] bytes) {
			return bytes;
		}

	}
}
