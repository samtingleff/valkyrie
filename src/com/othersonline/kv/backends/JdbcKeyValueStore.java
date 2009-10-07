package com.othersonline.kv.backends;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.backends.sql.DefaultJdbcDAO;
import com.othersonline.kv.backends.sql.JdbcDAO;
import com.othersonline.kv.backends.sql.KeyValuePair;
import com.othersonline.kv.backends.sql.SimpleDataSource;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.SerializingTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class JdbcKeyValueStore extends BaseManagedKeyValueStore implements
		KeyValueStore, IterableKeyValueStore {
	public static final String IDENTIFIER = "jdbc";

	private Log log = LogFactory.getLog(getClass());

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private JdbcDAO dao;

	private String dataSourceName;

	private String url;

	private String username;

	private String password;

	private DataSource ds;

	private String table;

	private String keyField;

	private String valueField;

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void setDataSource(DataSource ds) {
		this.ds = ds;
	}

	public void setDataSourceName(String name) {
		this.dataSourceName = name;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setKeyField(String keyField) {
		this.keyField = keyField;
	}

	public void setValueField(String valueField) {
		this.valueField = valueField;
	}

	public void start() throws IOException {
		try {
			if (ds == null) {
				if (dataSourceName == null) {
					this.ds = new SimpleDataSource(url, username, password);
				} else {
					Context ctx = new InitialContext();
					this.ds = (DataSource) ctx.lookup(dataSourceName);
				}
			}
		} catch (NamingException e) {
			throw new IOException(e);
		} finally {
		}
		dao = new DefaultJdbcDAO(table, keyField, valueField);
		super.start();
	}

	public void stop() {
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = dao.prepareExists(conn, key);
			rs = ps.executeQuery();
			if (rs.next()) {
				return true;
			} else
				return false;
		} catch (SQLException e) {
			throw new KeyValueStoreException(e);
		} finally {
			release(rs);
			release(ps);
			release(conn);
		}
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = dao.prepareSelect(conn, key);
			rs = ps.executeQuery();
			if (rs.next()) {
				KeyValuePair kp = dao.read(rs, transcoder);
				return kp.getValue();
			} else
				return null;
		} catch (SQLException e) {
			throw new KeyValueStoreException(e);
		} finally {
			release(rs);
			release(ps);
			release(conn);
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = dao.prepareBulkSelect(conn, keys);
			rs = ps.executeQuery();
			Map<String, Object> results = new HashMap<String, Object>();
			while (rs.next()) {
				KeyValuePair kp = dao.read(rs, defaultTranscoder);
				results.put(kp.getKey(), kp.getValue());
			}
			return results;
		} catch (SQLException e) {
			throw new KeyValueStoreException(e);
		} finally {
			release(rs);
			release(ps);
			release(conn);
		}
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		return getBulk(keys, defaultTranscoder);
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = dao.prepareBulkSelect(conn, keys);
			rs = ps.executeQuery();
			Map<String, Object> results = new HashMap<String, Object>();
			while (rs.next()) {
				KeyValuePair kp = dao.read(rs, transcoder);
				results.put(kp.getKey(), kp.getValue());
			}
			return results;
		} catch (SQLException e) {
			throw new KeyValueStoreException(e);
		} finally {
			release(rs);
			release(ps);
			release(conn);
		}
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = dao.prepareInsert(conn, key, value, transcoder);
			ps.execute();
			if (!conn.getAutoCommit())
				conn.commit();
		} catch (SQLException e) {
			throw new KeyValueStoreException(e);
		} finally {
			release(rs);
			release(ps);
			release(conn);
		}
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = dao.prepareDelete(conn, key);
			ps.execute();
			if (!conn.getAutoCommit())
				conn.commit();
		} catch (SQLException e) {
			throw new KeyValueStoreException(e);
		} finally {
			release(rs);
			release(ps);
			release(conn);
		}
	}

	public long size() throws KeyValueStoreException {
		assertReadable();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = dao.prepareCount(conn);
			rs = ps.executeQuery();
			if (rs.next()) {
				long count = rs.getLong(1);
				return count;
			} else
				return 0l;
		} catch (SQLException e) {
			throw new KeyValueStoreException(e);
		} finally {
			release(rs);
			release(ps);
			release(conn);
		}
	}

	public KeyValueStoreIterator iterkeys() throws KeyValueStoreException {
		assertReadable();
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			ps = dao.prepareIterator(conn);
			ResultSet rs = ps.executeQuery();
			return new JdbcKeyValueIterator(conn, ps, rs);
		} catch (SQLException e) {
			throw new KeyValueStoreException(e);
		} finally {
		}
	}

	private Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

	private void release(Connection conn) {
		if (conn != null)
			try {
				conn.close();
			} catch (Exception e) {
			}
	}

	private void release(PreparedStatement ps) {
		if (ps != null)
			try {
				ps.close();
			} catch (Exception e) {
			}
	}

	private void release(ResultSet rs) {
		if (rs != null)
			try {
				rs.close();
			} catch (Exception e) {
			}
	}

	private class JdbcKeyValueIterator implements KeyValueStoreIterator,
			Iterator<String> {

		private Connection conn;

		private PreparedStatement ps;

		private ResultSet rs;

		private String next;

		private JdbcKeyValueIterator(Connection conn, PreparedStatement ps,
				ResultSet rs) {
			this.conn = conn;
			this.ps = ps;
			this.rs = rs;
		}

		public Iterator<String> iterator() {
			return this;
		}

		public boolean hasNext() {
			try {
				return rs.next();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		public String next() {
			try {
				KeyValuePair kp = dao.read(rs, defaultTranscoder);
				this.next = kp.getKey();
				return this.next;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public void remove() {
			try {
				PreparedStatement ps = dao.prepareDelete(conn, next);
				ps.executeUpdate();
				if (!conn.getAutoCommit())
					conn.commit();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
			}
		}

		public void close() {
			release(rs);
			release(ps);
			release(conn);
		}

	}
}
