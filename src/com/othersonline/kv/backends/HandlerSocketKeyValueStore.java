package com.othersonline.kv.backends;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.backends.handlersocket.FindOperator;
import com.othersonline.kv.backends.handlersocket.HSClient;
import com.othersonline.kv.backends.handlersocket.exception.HandlerSocketException;
import com.othersonline.kv.backends.handlersocket.impl.HSClientImpl;
import com.othersonline.kv.transcoder.StringTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class HandlerSocketKeyValueStore extends BaseManagedKeyValueStore
		implements KeyValueStore {
	public static final String IDENTIFIER = "handlerSocket";

	protected Log log = LogFactory.getLog(getClass());

	protected Transcoder defaultTranscoder = new StringTranscoder();

	protected HSClient reader;

	protected HSClient writer;

	protected String host = "localhost";

	protected int readPort = 9998;

	protected int readPoolSize = 10;

	protected int writePort = 9999;

	protected int writePoolSize = 10;

	protected Serialization serializer;

	protected String db;

	protected String table;

	protected String index = "PRIMARY";

	protected String valueColumn = "value";

	protected boolean updateBeforeInsert = false;

	@Configurable(name = "host", accepts = Type.StringType)
	public void setHost(String host) {
		this.host = host;
	}

	@Configurable(name = "readPort", accepts = Type.IntType)
	public void setReadPort(int port) {
		this.readPort = port;
	}

	@Configurable(name = "readPoolSize", accepts = Type.IntType)
	public void setReadPoolSize(int readPoolSize) {
		this.readPoolSize = readPoolSize;
	}

	@Configurable(name = "writePort", accepts = Type.IntType)
	public void setWritePort(int port) {
		this.writePort = port;
	}

	@Configurable(name = "writePoolSize", accepts = Type.IntType)
	public void setSritePoolSize(int writePoolSize) {
		this.writePoolSize = writePoolSize;
	}

	@Configurable(name = "db", accepts = Type.StringType)
	public void setDb(String db) {
		this.db = db;
	}

	@Configurable(name = "table", accepts = Type.StringType)
	public void setTable(String table) {
		this.table = table;
	}

	@Configurable(name = "index", accepts = Type.StringType)
	public void setIndex(String index) {
		this.index = index;
	}

	@Configurable(name = "valueColumn", accepts = Type.StringType)
	public void setValueColumn(String valueColumn) {
		this.valueColumn = valueColumn;
	}

	@Configurable(name = "updateBeforeInsert", accepts = Type.BooleanType)
	public void setUpdateBeforeInsert(boolean updateBeforeInsert) {
		this.updateBeforeInsert = updateBeforeInsert;
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void start() throws IOException {
		serializer = new DefaultSerialization(this.valueColumn);
		try {
			this.reader = initReaderConnection();
			this.writer = initWriterConnection();
		} catch (InterruptedException e) {
			log.error("InterruptedException in start()");
			throw new IOException(e);
		} catch (TimeoutException e) {
			log.error("TimeoutException in start()");
			throw new IOException(e);
		} catch (HandlerSocketException e) {
			log.error("HandlerSocketException in start()");
			throw new IOException(e);
		} finally {
		}
		super.start();
	}

	@Override
	public void stop() {
		try {
			this.reader.shutdown();
			this.writer.shutdown();
		} catch (Exception e) {
			log.warn("Exception calling shutdown()", e);
		}
		super.stop();
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		Object obj = get(key);
		return obj != null;
	}

	@Override
	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		return get(key, defaultTranscoder);
	}

	@Override
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		HSClient client = null;
		ResultSet rs = null;
		try {
			client = getReaderConnection();
			rs = client.find(getIndexId(client, key), new String[] { key });
			Object result = null;
			if (rs.next()) {
				result = serializer.fetch(transcoder, rs);
				return result;
			}
		} catch (Exception e) {
			log.error("Exception reading", e);
		} finally {
			close(rs);
			close(client);
		}
		return null;
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return getBulk(Arrays.asList(keys));
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return getBulk(keys, defaultTranscoder);
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		HSClient client = null;
		try {
			Map<String, Object> results = new HashMap<String, Object>();
			client = getReaderConnection();
			for (String key : keys) {
				ResultSet rs = client.find(getIndexId(client, key),
						new String[] { key });
				try {
					if (rs.next()) {
						Object o = serializer.fetch(transcoder, rs);
						results.put(key, o);
					}
				} finally {
					close(rs);
				}
			}
			return results;
		} catch (Exception e) {
			log.error("Exception reading", e);
		} finally {
			close(client);
		}
		return null;
	}

	@Override
	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		set(key, value, defaultTranscoder);
	}

	@Override
	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		HSClient client = null;
		try {
			client = getWriterConnection();
			int indexId = getIndexId(client, key);
			int rows = 0;
			if (updateBeforeInsert) {
				// try an update
				rows = client.update(indexId,
						new String[] { key }, new byte[][] { transcoder
								.encode(value) }, FindOperator.EQ);
			}
			if (rows == 0) {
				byte[][] values = serializer.values(transcoder, key, value);
				boolean b = client.insert(indexId, values);
				rows = (b) ? 1 : 0;
				if (rows == 0) {
					// if updateBeforeInsert == false
					rows = client.update(indexId,
							new String[] { key }, new byte[][] { transcoder
									.encode(value) }, FindOperator.EQ);
				}
			}
		} catch (Exception e) {
			log.error("Exception calling set()", e);
		} finally {
		}
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		HSClient client = null;
		try {
			client = getWriterConnection();
			client.delete(getIndexId(client, key), new String[] { key });
		} catch (Exception e) {
			log.error("Exception calling set()", e);
		} finally {
		}

	}

	protected HSClient getReaderConnection() throws IOException,
			InterruptedException, TimeoutException, HandlerSocketException {
		return this.reader;
	}

	protected HSClient getWriterConnection() throws IOException,
			InterruptedException, TimeoutException, HandlerSocketException {
		return this.writer;
	}

	protected HSClient initReaderConnection() throws IOException,
			InterruptedException, TimeoutException, HandlerSocketException {
		HSClient client = new HSClientImpl(host, readPort, readPoolSize);
		openIndices(client);
		return client;
	}

	protected HSClient initWriterConnection() throws IOException,
			InterruptedException, TimeoutException, HandlerSocketException {
		HSClient client = new HSClientImpl(host, writePort, writePoolSize);
		openIndices(client);
		return client;
	}

	protected int getIndexId(HSClient client, String key) {
		return 0;
	}

	protected void openIndices(HSClient client) throws InterruptedException,
			TimeoutException, HandlerSocketException {
		client.openIndex(0, db, table, index, serializer.valueColumns());
	}

	protected void close(HSClient client) {
	}

	protected void close(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				log.warn("Exception calling ResultSet.close()", e);
			}
		}
	}

	public static interface Serialization {
		public String[] valueColumns();

		public Object fetch(Transcoder transcoder, ResultSet rs)
				throws SQLException, IOException;

		/**
		 * Return a byte[][] suitable for writes to db.
		 * 
		 * @param key
		 * @param value
		 * @return
		 * @throws IOException
		 */
		public byte[][] values(Transcoder transcoder, String key, Object value)
				throws IOException;

	}

	public static class DefaultSerialization implements Serialization {
		private String valueColumn;

		public DefaultSerialization(String valueColumn) {
			this.valueColumn = valueColumn;
		}

		public String[] valueColumns() {
			return new String[] { valueColumn };
		}

		public Object fetch(Transcoder transcoder, ResultSet rs)
				throws SQLException, IOException {
			byte[] bytes = rs.getBytes(1);
			Object obj = transcoder.decode(bytes);
			return obj;
		}

		public byte[][] values(Transcoder transcoder, String key, Object value)
				throws IOException {
			byte[][] values = new byte[][] { key.getBytes(),
					transcoder.encode(value) };
			return values;
		}

	}
}
