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

	private ObjectPool connectionPool;

	private tokyotyrant.transcoder.Transcoder tokyoDefaultTranscoder = new SerializingTranscoder();

	private tokyotyrant.transcoder.Transcoder tokyoByteTranscoder = new ByteArrayTranscoder();

	private String host = "localhost";

	private int port = 1978;

	public TokyoTyrantKeyValueStore() {
	}

	public TokyoTyrantKeyValueStore(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		connectionPool = new StackObjectPool(new RDBConnectionFactory(host,
				port), 10, 10);
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
		private String host;

		private int port;

		public RDBConnectionFactory(String host, int port) {
			this.host = host;
			this.port = port;
		}

		public Object makeObject() throws Exception {
			RDB rdb = new RDB();
			rdb.open(new InetSocketAddress(host, port));
			return rdb;
		}

	}
}
