package com.othersonline.kv.backends;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.ManagedKeyValueStore;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.gen.Constants;
import com.othersonline.kv.gen.GetResult;
import com.othersonline.kv.gen.KeyValueService;
import com.othersonline.kv.gen.KeyValueStoreIOException;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class ThriftKeyValueStore extends BaseManagedKeyValueStore implements
		ManagedKeyValueStore {
	public static final String IDENTIFIER = "thrift";

	private Log log = LogFactory.getLog(getClass());

	private GenericObjectPool connectionPool;

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private String host = "localhost";

	private int port = Constants.DEFAULT_PORT;

	private boolean lifo = true;

	private int maxActive = 100;

	private int maxIdle = 100;

	private long maxWait = -1;

	private boolean testWhileIdle = false;

	private long timeBetweenEvictionRunsMillis = -1;

	public ThriftKeyValueStore() {
	}

	public ThriftKeyValueStore(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@Configurable(name = "host", accepts = Type.StringType)
	public void setHost(String host) {
		this.host = host;
	}

	@Configurable(name = "port", accepts = Type.IntType)
	public void setPort(int port) {
		this.port = port;
	}

	@Configurable(name = "lifo", accepts = Type.BooleanType)
	public void setLifo(boolean lifo) {
		this.lifo = lifo;
	}

	@Configurable(name = "maxActive", accepts = Type.IntType)
	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	@Configurable(name = "maxIdle", accepts = Type.IntType)
	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	@Configurable(name = "testWhileIdle", accepts = Type.LongType)
	public void setMaxWait(long maxWait) {
		this.maxWait = maxWait;
	}

	@Configurable(name = "testWhileIdle", accepts = Type.BooleanType)
	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}

	@Configurable(name = "timeBetweenEvictionRunsMillis", accepts = Type.LongType)
	public void setTimeBetweenEvictionRunsMillis(long millis) {
		this.timeBetweenEvictionRunsMillis = millis;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		log.trace("start()");
		connectionPool = new GenericObjectPool(new TConnectionFactory(host,
				port), maxActive, GenericObjectPool.WHEN_EXHAUSTED_FAIL,
				maxWait, maxIdle);
		connectionPool.setLifo(lifo);
		super.start();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		log.trace("exists()");
		assertReadable();
		TConnection tconn = null;
		try {
			tconn = getTConnection();
			boolean e = tconn.kv.exists(key);
			return e;
		} catch (TTransportException e) {
			log.error("TTransportException inside exists()", e);
			throw new IOException(e);
		} catch (KeyValueStoreIOException e) {
			log.error("KeyValueStoreIOException inside exists()", e);
			throw new IOException(e);
		} catch (com.othersonline.kv.gen.KeyValueStoreException e) {
			log.error("KeyValueStoreException inside exists()", e);
			throw new KeyValueStoreException(e);
		} catch (TException e) {
			log.error("TException inside exists()", e);
			throw new IOException(e);
		} catch (Exception e) {
			log.error("Exception inside exists()", e);
			throw new IOException(e);
		} finally {
			closeTConnection(tconn);
		}
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		log.trace("get()");
		assertReadable();
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		log.trace("get()");
		assertReadable();
		TConnection tconn = null;
		try {
			tconn = getTConnection();
			GetResult result = tconn.kv.getValue(key);
			if (!result.isExists())
				return null;
			else {
				byte[] data = result.getData();
				Object obj = transcoder.decode(data);
				return obj;
			}
		} catch (TTransportException e) {
			log.error("TTransportException inside get()", e);
			throw new IOException(e);
		} catch (KeyValueStoreIOException e) {
			log.error("KeyValueStoreIOException inside get()", e);
			throw new IOException(e);
		} catch (com.othersonline.kv.gen.KeyValueStoreException e) {
			log.error("KeyValueStoreException inside get()", e);
			throw new KeyValueStoreException(e);
		} catch (TException e) {
			log.error("TException inside get()", e);
			throw new IOException(e);
		} catch (Exception e) {
			log.error("Exception inside get()", e);
			throw new IOException(e);
		} finally {
			closeTConnection(tconn);
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		List<String> coll = Arrays.asList(keys);
		return getBulk(coll);
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		log.trace("getBulk()");
		return getBulk(keys, defaultTranscoder);
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		log.trace("getBulk()");
		assertReadable();
		TConnection tconn = null;
		try {
			tconn = getTConnection();
			Map<String, GetResult> results = tconn.kv.getBulk(keys);
			Map<String, Object> retval = new HashMap<String, Object>(results
					.size());
			for (Map.Entry<String, GetResult> entry : results.entrySet()) {
				byte[] data = entry.getValue().getData();
				Object obj = transcoder.decode(data);
				retval.put(entry.getKey(), obj);
			}
			return retval;
		} catch (TTransportException e) {
			log.error("TTransportException inside getBulk()", e);
			throw new IOException(e);
		} catch (KeyValueStoreIOException e) {
			log.error("KeyValueStoreIOException inside getBulk()", e);
			throw new IOException(e);
		} catch (com.othersonline.kv.gen.KeyValueStoreException e) {
			log.error("KeyValueStoreException inside getBulk()", e);
			throw new KeyValueStoreException(e);
		} catch (TException e) {
			log.error("TException inside getBulk()", e);
			throw new IOException(e);
		} catch (Exception e) {
			log.error("Exception inside getBulk()", e);
			throw new IOException(e);
		} finally {
			closeTConnection(tconn);
		}
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		log.trace("set()");
		assertWriteable();
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		log.trace("set()");
		assertWriteable();
		TConnection tconn = null;
		try {
			tconn = getTConnection();
			byte[] data = transcoder.encode(value);
			ByteBuffer buff = ByteBuffer.wrap(data);
			tconn.kv.setValue(key, buff);
		} catch (TTransportException e) {
			log.error("TTransportException inside set()", e);
			throw new IOException(e);
		} catch (KeyValueStoreIOException e) {
			log.error("KeyValueStoreIOException inside set()", e);
			throw new IOException(e);
		} catch (com.othersonline.kv.gen.KeyValueStoreException e) {
			log.error("KeyValueStoreException inside set()", e);
			throw new KeyValueStoreException(e);
		} catch (TException e) {
			log.error("TException inside set()", e);
			throw new IOException(e);
		} catch (Exception e) {
			log.error("Exception inside set()", e);
			throw new IOException(e);
		} finally {
			closeTConnection(tconn);
		}
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		log.trace("delete()");
		assertWriteable();
		TConnection tconn = null;
		try {
			tconn = getTConnection();
			tconn.kv.deleteValue(key);
		} catch (TTransportException e) {
			log.error("TTransportException inside delete()", e);
			throw new IOException(e);
		} catch (KeyValueStoreIOException e) {
			log.error("KeyValueStoreIOException inside delete()", e);
			throw new IOException(e);
		} catch (com.othersonline.kv.gen.KeyValueStoreException e) {
			log.error("KeyValueStoreException inside delete()", e);
			throw new KeyValueStoreException(e);
		} catch (TException e) {
			log.error("TException inside delete()", e);
			throw new IOException(e);
		} catch (Exception e) {
			log.error("Exception inside delete()", e);
			throw new IOException(e);
		} finally {
			closeTConnection(tconn);
		}
	}

	private TConnection getTConnection() throws Exception {
		log.trace("connect()");
		TConnection tc = (TConnection) connectionPool.borrowObject();
		return tc;
	}

	private void closeTConnection(TConnection tconn) {
		log.trace("disconnect()");
		try {
			connectionPool.returnObject(tconn);
		} catch (Exception e) {
			log.warn("Exception calling transport.close()", e);
		}
	}

	private static class TConnectionFactory extends BasePoolableObjectFactory {
		private String server;

		private int port;

		public TConnectionFactory(String server, int port) {
			this.server = server;
			this.port = port;
		}

		/**
		 * Create a new object.
		 */
		public Object makeObject() throws Exception {
			TSocket socket = new TSocket(server, port);
			TFramedTransport framed = new TFramedTransport(socket);
			TProtocol protocol = new TBinaryProtocol(framed);
			KeyValueService.Iface kv = new KeyValueService.Client(protocol);
			framed.open();
			return new TConnection(socket, kv);
		}

		/**
		 * Uninitialize an instance to be returned to the pool.
		 * 
		 * @param obj
		 *            the instance to be passivated
		 */
		public void passivateObject(Object obj) throws Exception {
		}

	}

	private static class TConnection {
		public TTransport transport;

		public KeyValueService.Iface kv;

		public TConnection(TTransport transport, KeyValueService.Iface kv) {
			this.transport = transport;
			this.kv = kv;
		}
	}
}
