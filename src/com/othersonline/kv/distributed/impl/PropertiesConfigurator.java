package com.othersonline.kv.distributed.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.othersonline.kv.backends.ConnectionFactory;
import com.othersonline.kv.backends.UriConnectionFactory;
import com.othersonline.kv.distributed.Configuration;
import com.othersonline.kv.distributed.Configurator;
import com.othersonline.kv.distributed.NodeStore;

public class PropertiesConfigurator implements Configurator {
	public static final String SYNC_OP_THREAD_POOL = "syncpool.threads";

	public static final String SYNC_OP_MAX_QUEUE_SIZE = "syncpool.maxQueueSize";

	public static final String ASYNC_OP_THREAD_POOL = "asyncpool.threads";

	public static final String ASYNC_OP_MAX_QUEUE_SIZE = "asyncpool.maxQueueSize";

	public static final String MAX_NODE_ERROR_COUNT = "node.maxErrorCount";

	public static final String NODE_STORE = "nodestore.implementation";

	public static final String NODE_STORE_URL = "nodestore.url";

	public static final String READ_OPERATION_TIMEOUT = "read.timeout";

	public static final String READ_REPLICAS = "read.replicas";

	public static final String REQUIRED_READS = "read.required";

	public static final String REQUIRED_WRITES = "write.required";

	public static final String WRITE_OPERATION_TIMEOUT = "write.timeout";

	public static final String WRITE_REPLICAS = "write.replicas";

	public static final String BACKFILL_NULL_GET_REQUESTS = "backfill.nullGets";

	public static final String BACKFILL_FAILED_GET_REQUESTS = "backfill.failedGets";

	private volatile Configuration config;

	public PropertiesConfigurator() {
	}

	public PropertiesConfigurator(File file) throws IOException {
		load(file);
	}

	public PropertiesConfigurator(InputStream is) throws IOException {
		load(is);
	}

	public PropertiesConfigurator(Properties p) throws IOException {
		load(p);
	}

	public void load(File file) throws IOException {
		FileInputStream fis = new FileInputStream(file);
		try {
			load(fis);
		} finally {
			fis.close();
		}
	}

	public void load(InputStream in) throws IOException {
		Properties p = new Properties();
		p.load(in);
		load(p);
	}

	public void load(Properties props) throws IOException {
		this.config = getConfig(props);
	}

	public Configuration getConfiguration() throws IOException {
		return config;
	}

	private Configuration getConfig(Properties p)
			throws IllegalArgumentException {
		ConnectionFactory cf = new UriConnectionFactory();
		int syncOperationThreadPoolCount = getIntProperty(p,
				SYNC_OP_THREAD_POOL, 100);
		int syncOperationMaxQueueSize = getIntProperty(p,
				SYNC_OP_MAX_QUEUE_SIZE, 100);
		int asyncOperationThreadPoolCount = getIntProperty(p,
				ASYNC_OP_THREAD_POOL, 10);
		int asyncOperationMaxQueueSize = getIntProperty(p,
				ASYNC_OP_MAX_QUEUE_SIZE, 100);

		Configuration config = new Configuration();
		config
				.setAsyncOperationQueue(new NonPersistentThreadPoolOperationQueue(
						p, cf, asyncOperationThreadPoolCount,
						syncOperationMaxQueueSize));
		config.setConnectionFactory(cf);
		config
				.setMaxNodeErrorCount(getIntProperty(p, MAX_NODE_ERROR_COUNT,
						100));
		config.setNodeErrorCountPeriod(TimeUnit.MINUTES);
		config.setNodeStore(getNodeStore(NODE_STORE, p));
		config.setReadOperationTimeout(getIntProperty(p,
				READ_OPERATION_TIMEOUT, 500));
		config.setReadReplicas(getIntProperty(p, READ_REPLICAS, 3));
		config.setRequiredReads(getIntProperty(p, REQUIRED_READS, 2));
		config.setRequiredWrites(getIntProperty(p, REQUIRED_WRITES, 2));
		config
				.setSyncOperationQueue(new NonPersistentThreadPoolOperationQueue(
						p, cf, syncOperationThreadPoolCount,
						asyncOperationMaxQueueSize));
		config.setWriteOperationTimeout(getIntProperty(p,
				WRITE_OPERATION_TIMEOUT, 500));
		config.setWriteReplicas(getIntProperty(p, WRITE_REPLICAS, 3));
		config.setFillNullGetResults(getBooleanProperty(p,
				BACKFILL_NULL_GET_REQUESTS, true));
		config.setFillErrorGetResults(getBooleanProperty(p,
				BACKFILL_FAILED_GET_REQUESTS, false));
		return config;
	}

	private NodeStore getNodeStore(String name, Properties props)
			throws IllegalArgumentException {
		try {
			Class<NodeStore> cls = (Class<NodeStore>) Class.forName(props
					.getProperty(NODE_STORE));
			Object obj = cls.newInstance();
			NodeStore ns = (NodeStore) obj;
			ns.setProperties(props);
			return ns;
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	private int getIntProperty(Properties p, String name, int defaultValue) {
		String value = p.getProperty(name);
		if (value == null) {
			value = Integer.toString(defaultValue);
		}
		return Integer.parseInt(value);
	}

	public boolean getBooleanProperty(Properties p, String name,
			boolean defaultValue) {
		String value = p.getProperty(name);
		if (value == null) {
			value = Boolean.toString(defaultValue);
		}
		return Boolean.parseBoolean(value);
	}
}
