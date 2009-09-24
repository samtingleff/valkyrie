package com.othersonline.kv.backends;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import tokyotyrant.RDB;
import tokyotyrant.transcoder.ByteArrayTranscoder;
import tokyotyrant.transcoder.SerializingTranscoder;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.transcoder.Transcoder;

public class TokyoTyrantKeyValueStore extends BaseManagedKeyValueStore
		implements KeyValueStore, IterableKeyValueStore {
	public enum Locking {
		Record(RDB.XOLCKREC), Global(RDB.XOLCKGLB);
		private int rdbLockPolicy;

		Locking(int rdbLockPolicy) {
			this.rdbLockPolicy = rdbLockPolicy;
		}

		int getRDBLockPolicy() {
			return rdbLockPolicy;
		}
	}

	public static final int OPTS_RECORD_LOCKING = RDB.XOLCKREC;

	public static final int OPTS_GLOBAL_LOCKING = RDB.XOLCKGLB;

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

	private int initialConnections = 10;

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

	@Configurable(name = "host", accepts = Type.StringType)
	public void setHost(String host) {
		this.host = host;
	}

	@Configurable(name = "port", accepts = Type.IntType)
	public void setPort(int port) {
		this.port = port;
	}

	@Configurable(name = "socketTimeout", accepts = Type.IntType)
	public void setSocketTimeout(int millis) {
		this.socketTimeout = millis;
	}

	@Configurable(name = "maxActive", accepts = Type.IntType)
	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	@Configurable(name = "maxIdle", accepts = Type.IntType)
	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	@Configurable(name = "initialConnections", accepts = Type.IntType)
	public void setInitialConnections(int initialConnections) {
		this.initialConnections = initialConnections;
	}

	@Configurable(name = "maxWait", accepts = Type.LongType)
	public void setMaxWait(long maxWait) {
		this.maxWait = maxWait;
	}

	@Configurable(name = "timeBetweenEvictionRunsMillis", accepts = Type.LongType)
	public void setTimeBetweenEvictionRunsMillis(
			long timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	@Configurable(name = "numTestsPerEvictionRun", accepts = Type.IntType)
	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	@Configurable(name = "minEvictableIdleTimeMillis", accepts = Type.IntType)
	public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		log.warn("Creating connection pool.");
		connectionPool = new GenericObjectPool(new RDBConnectionFactory(host,
				port, socketTimeout), maxActive,
				GenericObjectPool.WHEN_EXHAUSTED_BLOCK, maxWait, maxIdle,
				false, false, timeBetweenEvictionRunsMillis,
				numTestsPerEvictionRun, minEvictableIdleTimeMillis, true);
		// pre-connect to # of initialConnections
		List<RDB> list = new ArrayList<RDB>(initialConnections);
		for (int i = 0; i < initialConnections; ++i) {
			try {
				RDB rdb = getRDB();
				list.add(rdb);
			} catch (Exception e) {
				log.error("Could not open connection inside start()", e);
			}
		}
		for (RDB rdb : list) {
			try {
				releaseRDB(rdb);
			} catch (Exception e) {
				log.error("Could not close connection inside start()", e);
			}
		}
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
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside exists(): "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
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
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("Unable to get value for key. Socket timeout on " + host
					+ ":" + port + " " + e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Unable to get value for key. " + host + ":" + port + " "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			byte[] bytes = (byte[]) rdb.get(key, tokyoByteTranscoder);
			if (bytes == null)
				return null;
			else
				return transcoder.decode(bytes);
		} catch (NoSuchElementException e) {
			log.error("Unable to get value for key. No such element. "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("Unable to get value for key. Socket timeout on " + host
					+ ":" + port + " " + e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Unable to get value for key. " + host + ":" + port + " "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			Map results = rdb.mget(keys, tokyoDefaultTranscoder);
			return results;
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside getBulk(): "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside getBulk()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Map<String, Object> getBulk(final List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			Map results = rdb.mget(keys.toArray(new String[keys.size()]),
					tokyoDefaultTranscoder);
			return results;
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside getBulk(): "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside getBulk()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Map<String, Object> getBulk(final List<String> keys,
			Transcoder transcoder) throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = null;
		RDB rdb = null;
		try {
			rdb = getRDB();
			Map byteResults = rdb.mget(keys.toArray(new String[keys.size()]),
					tokyoByteTranscoder);
			results = new HashMap<String, Object>(byteResults.size());

			Set<Map.Entry<String, byte[]>> set = byteResults.entrySet();
			for (Map.Entry<String, byte[]> entry : set) {
				Object obj = transcoder.decode(entry.getValue());
				results.put(entry.getKey(), obj);
			}
			return results;
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside getBulk(): "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside getBulk()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			rdb.put(key, value, tokyoDefaultTranscoder);
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside set(): " + e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Unable to set value for key. " + e.getMessage());
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			byte[] bytes = transcoder.encode(value);
			rdb.put(key, bytes, tokyoByteTranscoder);
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside set(): " + e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Unable to set value for key. " + e.getMessage());
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
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside delete(): "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside delete()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Map<String, String> getStats() throws KeyValueStoreException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			return rdb.stat();
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside getStats(): "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside getStats()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public long rnum() throws KeyValueStoreException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			return rdb.rnum();
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log
					.error("SocketTimeoutException inside rnum(): "
							+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside rnum()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public long size() throws KeyValueStoreException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			return rdb.size();
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log
					.error("SocketTimeoutException inside size(): "
							+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside size()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public boolean optimize() throws KeyValueStoreException {
		assertWriteable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			return rdb.optimize("");
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside optimize(): "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside optimize()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public KeyValueStoreIterator iterkeys() throws KeyValueStoreException {
		assertWriteable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			TokyoTyrantKeyIterator iter = new TokyoTyrantKeyIterator(rdb);
			iter.iterinit();
			return iter;
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside fwmkeys(): "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside fwmkeys()", e);
			throw new KeyValueStoreException(e);
		} finally {
		}
	}

	public Object[] fwmkeys(String prefix, int max)
			throws KeyValueStoreException {
		assertReadable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			return rdb.fwmkeys(prefix, max);
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside fwmkeys(): "
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside fwmkeys()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Object ext(String name, Object key, Object value, Locking locking)
			throws KeyValueStoreException {
		assertWriteable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			return rdb.ext(name, key, value, locking.getRDBLockPolicy());
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside ext(): " + e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside ext()", e);
			throw new KeyValueStoreException(e);
		} finally {
			releaseRDB(rdb);
		}
	}

	public Object ext(String name, Object key, Object value, Locking locking,
			Transcoder transcoder) throws KeyValueStoreException {
		assertWriteable();
		RDB rdb = null;
		try {
			rdb = getRDB();
			Object obj = rdb.ext(name, key, value, locking.getRDBLockPolicy(),
					tokyoByteTranscoder);
			if (obj != null) {
				byte[] bytes = (byte[]) obj;
				Object result = transcoder.decode(bytes);
				return result;
			} else
				return null;
		} catch (NoSuchElementException e) {
			log.error("NoSuchElementException waiting for connection:"
					+ e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (SocketTimeoutException e) {
			log.error("SocketTimeoutException inside ext(): " + e.getMessage());
			throw new KeyValueStoreException(e);
		} catch (Exception e) {
			log.error("Exception inside ext()", e);
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

	private class TokyoTyrantKeyIterator implements KeyValueStoreIterator,
			Iterator<String> {
		private RDB rdb;

		private String next;

		public TokyoTyrantKeyIterator(RDB rdb) {
			this.rdb = rdb;
		}

		public boolean iterinit() throws IOException {
			return rdb.iterinit();
		}

		public void close() {
			releaseRDB(rdb);
		}

		public Iterator<String> iterator() {
			return this;
		}

		public boolean hasNext() {
			try {
				next = (String) rdb.iternext();
				return (next != null);
			} catch (IOException e) {
				log.error("IOException calling hasNext()", e);
				return false;
			}
		}

		public String next() {
			return next;
		}

		public void remove() {
			try {
				rdb.out(next);
			} catch (IOException e) {
				log.error("IOException calling remove()", e);
				throw new RuntimeException(e);
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
			try {
				log.info("Creating connection to " + host + ":" + port);
				rdb.open(new InetSocketAddress(host, port), socketTimeout);
			} catch (ConnectException e) {
				log.error(String.format("Could not connect to %1$s:%2$d", host,
						port));
				throw e;
			}
			return rdb;
		}

		public void destroyObject(Object obj) {
			try {
				log.info("Closing connection to " + host + ":" + port);
				RDB rdb = (RDB) obj;
				rdb.close();
			} catch (Exception e) {
				log.error("destroyObject failed:" + e.getMessage());
			}
		}

		public boolean validateObject(Object obj) {
			boolean result = false;
			try {
				log.info("Validating connection to " + host + ":" + port);
				RDB rdb = (RDB) obj;
				long rnum = rdb.rnum();
				result = true;
			} catch (SocketTimeoutException e) {
				log
						.error("validateObject() failed due to java.net.SocketTimeoutException. Connection to Tokyo Tyrant is broken. "
								+ e.getMessage());
				result = false;
			} catch (Exception e) {
				log
						.error(
								"validateObject() failed. Connection to Tokyo Tyrant is broken. ",
								e);
				result = false;
			}
			return result;
		}

	}
}
