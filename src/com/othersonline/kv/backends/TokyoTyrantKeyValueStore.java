package com.othersonline.kv.backends;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;

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

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void start() throws IOException {
		connectionPool = new StackObjectPool(new RDBConnectionFactory(host,
				port), 10, 10);
		super.start();
	}

	@Override
	public void stop() {
		try {
			connectionPool.close();
		} catch (Exception e) {
		}
		super.stop();
	}

	@Override
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

	@Override
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

	@Override
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

	@Override
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

	@Override
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

	@Override
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

		@Override
		public Object makeObject() throws Exception {
			RDB rdb = new RDB();
			rdb.open(new InetSocketAddress(host, port));
			return rdb;
		}

		@Override
		public void passivateObject(Object obj) throws Exception {

		}

	}
}
