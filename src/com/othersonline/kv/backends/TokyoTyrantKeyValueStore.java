package com.othersonline.kv.backends;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;

import tokyotyrant.RDB;
import tokyotyrant.transcoder.ByteArrayTranscoder;
import tokyotyrant.transcoder.SerializingTranscoder;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.transcoder.Transcoder;

public class TokyoTyrantKeyValueStore extends BaseManagedKeyValueStore
		implements KeyValueStore {
	public static final String IDENTIFIER = "tyrant";

	private Log log = LogFactory.getLog(getClass());

	private GenericObjectPool connectionPool;

	private tokyotyrant.transcoder.Transcoder tokyoDefaultTranscoder = new SerializingTranscoder();

	private tokyotyrant.transcoder.Transcoder tokyoByteTranscoder = new ByteArrayTranscoder();

	private String host = "localhost";

	private int port = 1978;

	private int socketTimeout = 2000;

	private int maxActive = 10;

	private int maxIdle = 10;

	private long maxWait = 100l;

	private long timeBetweenEvictionRunsMillis = 10000l;

	private int numTestsPerEvictionRun = 3;

	private long minEvictableIdleTimeMillis = -1;

	public TokyoTyrantKeyValueStore() {
	}

	public TokyoTyrantKeyValueStore(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public TokyoTyrantKeyValueStore(String host, int port, int socketTimeout) {
		this.host = host;
		this.port = port;
		this.socketTimeout = socketTimeout;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setSocketTimeout(int millis) {
		this.socketTimeout = millis;
	}

	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public void setMaxWait(long maxWait) {
		this.maxWait = maxWait;
	}

	public void setTimeBetweenEvictionRunsMillis(
			long timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		connectionPool = new GenericObjectPool(new RDBConnectionFactory(host,
				port, socketTimeout), maxActive,
				GenericObjectPool.WHEN_EXHAUSTED_BLOCK, maxWait, maxIdle,
				false, false, timeBetweenEvictionRunsMillis,
				numTestsPerEvictionRun, minEvictableIdleTimeMillis, true);
		super.start();
	}

	public void stop() {
		try {
			connectionPool.close();
		} catch (Exception e) {
		}
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			return (rdb.get(key) != null);
		} catch (Exception e) {
			log.error("Exception inside exists()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			Object obj = rdb.get(key, tokyoDefaultTranscoder);
			return obj;
		} catch (Exception e) {
			log.error("Exception inside get()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			byte[] bytes = (byte[]) rdb.get(key, tokyoByteTranscoder);
			if (bytes == null)
				return null;
			else
				return transcoder.decode(bytes);
		} catch (Exception e) {
			log.error("Exception inside get()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		RDB rdb = null;
		try {
			rdb = getRDB();
			for (String key : keys) {
				Object obj = rdb.get(key, tokyoDefaultTranscoder);
				if (obj != null)
					results.put(key, obj);
			}
			return results;
		} catch (Exception e) {
			log.error("Exception inside getBulk()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Map<String, Object> getBulk(final List<String> keys)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		RDB rdb = null;
		try {
			rdb = getRDB();
			for (String key : keys) {
				Object obj = rdb.get(key, tokyoDefaultTranscoder);
				if (obj != null)
					results.put(key, obj);
			}
			return results;
		} catch (Exception e) {
			log.error("Exception inside getBulk()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Map<String, Object> getBulk(final List<String> keys,
			Transcoder transcoder) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		RDB rdb = null;
		try {
			rdb = getRDB();
			for (String key : keys) {
				byte[] bytes = (byte[]) rdb.get(key, tokyoByteTranscoder);
				if (bytes != null) {
					Object obj = transcoder.decode(bytes);
					results.put(key, obj);
				}
			}
			return results;
		} catch (Exception e) {
			log.error("Exception inside getBulk()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public void set(String key, Serializable value)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			rdb.put(key, value, tokyoDefaultTranscoder);
		} catch (Exception e) {
			log.error("Exception inside get()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public void set(String key, Serializable value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			byte[] bytes = transcoder.encode(value);
			rdb.put(key, bytes, tokyoByteTranscoder);
		} catch (Exception e) {
			log.error("Exception inside set()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			rdb.out(key);
		} catch (Exception e) {
			log.error("Exception inside delete()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	private RDB getRDB() throws Exception {
		RDB rdb = (RDB) connectionPool.borrowObject();
		return rdb;
	}

	private void releaseRDB(RDB rdb) {
		if (rdb != null) {
			try {
				connectionPool.returnObject(rdb);
			} catch (Exception e) {
			}
		}
	}

	private static class RDBConnectionFactory extends BasePoolableObjectFactory {
		private Log log = LogFactory.getLog(getClass());

		private String host;

		private int port;

		private int socketTimeout;

		public RDBConnectionFactory(String host, int port, int socketTimeout) {
			this.host = host;
			this.port = port;
			this.socketTimeout = socketTimeout;
		}

		public Object makeObject() throws Exception {
			RDB rdb = new RDB();
			rdb.open(new InetSocketAddress(host, port), socketTimeout);
			return rdb;
		}

		public boolean validateObject(Object obj) {
			boolean result = false;
			try {
				RDB rdb = (RDB) obj;
				long rnum = rdb.rnum();
				result = true;
			} catch (Exception e) {
				log
						.error(
								"validateObject() failed. Connection to Tokyo Tyrant is broken",
								e);
				result = false;
			}
			return result;
		}

	}
}
