package com.othersonline.kv.backends;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.Column;
import org.apache.cassandra.service.ColumnOrSuperColumn;
import org.apache.cassandra.service.ColumnPath;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.NotFoundException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.transcoder.SerializingTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class CassandraKeyValueStore extends BaseManagedKeyValueStore {

	public static final String IDENTIFIER = "cassandra";

	private Transcoder defaultTranscoder = new SerializingTranscoder();

	private Cassandra.Client client;

	private String host = "localhost";

	private int port = 9160;

	private String keyspace = "Keyspace1";

	private String columnFamily = "Standard1";

	private String column = "default-column";

	private int writeConsistencyLevel = ConsistencyLevel.ONE;

	private int readConsistencyLevel = ConsistencyLevel.ONE;

	private byte[] columnBytes = null;

	private ColumnPath columnPath;

	@Configurable(name = "host", accepts = Type.StringType)
	public void setHost(String host) {
		this.host = host;
	}

	@Configurable(name = "port", accepts = Type.IntType)
	public void setPort(int port) {
		this.port = port;
	}

	@Configurable(name = "keyspace", accepts = Type.StringType)
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	@Configurable(name = "columnFamily", accepts = Type.StringType)
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	@Configurable(name = "column", accepts = Type.StringType)
	public void setColumn(String column) {
		this.column = column;
	}

	@Configurable(name = "writeConsistencyLevel", accepts = Type.IntType)
	public void setWriteConsistencyLevel(int level) {
		this.writeConsistencyLevel = level;
	}

	@Configurable(name = "readConsistencyLevel", accepts = Type.IntType)
	public void setReadConsistencyLevel(int level) {
		this.readConsistencyLevel = level;
	}

	public void start() throws IOException {
		try {
			columnBytes = column.getBytes("UTF-8");
			TTransport tr = new TSocket(host, port);
			TProtocol proto = new TBinaryProtocol(tr);
			this.client = new Cassandra.Client(proto);
			tr.open();
			columnPath = new ColumnPath(columnFamily, null, columnBytes);
		} catch (TTransportException e) {
			throw new IOException(e);
		} finally {
		}

		super.start();
	}

	public void stop() {
		super.stop();
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		try {
			ColumnOrSuperColumn val = client.get(keyspace, key, columnPath,
					readConsistencyLevel);
			return true;
		} catch (NotFoundException e) {
			return false;
		} catch (Exception e) {
			throw new KeyValueStoreException(e);
		} finally {
		}
	}

	@Override
	public Object get(String key) throws KeyValueStoreException, IOException {
		return get(key, defaultTranscoder);
	}

	@Override
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		try {
			ColumnOrSuperColumn val = client.get(keyspace, key, columnPath,
					readConsistencyLevel);
			Column col = val.getColumn();
			byte[] bytes = col.getValue();
			Object obj = transcoder.decode(bytes);
			return obj;
		} catch (NotFoundException e) {
			return null;
		} catch (Exception e) {
			throw new KeyValueStoreException(e);
		} finally {
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		return getBulk(Arrays.asList(keys));
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		return getBulk(keys, defaultTranscoder);
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> values = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key, transcoder);
			if (obj != null)
				values.put(key, obj);
		}
		return values;
	}

	@Override
	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		set(key, value, defaultTranscoder);
	}

	@Override
	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		try {
			byte[] bytes = transcoder.encode(value);
			client.insert(keyspace, key, new ColumnPath(columnFamily, null,
					columnBytes), bytes, System.currentTimeMillis(),
					writeConsistencyLevel);
		} catch (Exception e) {
			throw new KeyValueStoreException(e);
		} finally {
		}
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		try {
			client.remove(keyspace, key, columnPath,
					System.currentTimeMillis(), writeConsistencyLevel);
		} catch (Exception e) {
			throw new KeyValueStoreException(e);
		} finally {
		}
	}

}
