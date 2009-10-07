package com.othersonline.kv.backends.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.othersonline.kv.transcoder.Transcoder;

public class DefaultJdbcDAO implements JdbcDAO {

	private String table;

	private String keyField;

	private String valueField;

	public DefaultJdbcDAO(String table, String keyField, String valueField) {
		this.table = table;
		this.keyField = keyField;
		this.valueField = valueField;
	}

	public PreparedStatement prepareExists(Connection conn, String key)
			throws SQLException {
		PreparedStatement ps = conn.prepareStatement(String.format(
				"select %1$s from %2$s where %1$s = ?", keyField, table));
		ps.setString(1, key);
		return ps;
	}

	public PreparedStatement prepareSelect(Connection conn, String key)
			throws SQLException {
		PreparedStatement ps = conn.prepareStatement(String.format(
				"select %3$s, %1$s from %2$s where %3$s = ?", valueField,
				table, keyField));
		ps.setString(1, key);
		return ps;
	}

	public PreparedStatement prepareBulkSelect(Connection conn, String... keys)
			throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("select %1$s, %2$s from %3$s where ");
		for (int i = 0; i < keys.length; ++i) {
			if (i > 0)
				sb.append(" or ");
			sb.append("%1$s = ?");
		}
		String query = String
				.format(sb.toString(), keyField, valueField, table);
		PreparedStatement ps = conn.prepareStatement(query);
		for (int i = 0; i < keys.length; ++i)
			ps.setString(i + 1, keys[i]);
		return ps;
	}

	public PreparedStatement prepareBulkSelect(Connection conn,
			List<String> keys) throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("select %1$s, %2$s from %3$s where ");
		for (int i = 0; i < keys.size(); ++i) {
			if (i > 0)
				sb.append(" or ");
			sb.append("%1$s = ?");
		}
		String query = String
				.format(sb.toString(), keyField, valueField, table);
		PreparedStatement ps = conn.prepareStatement(query);
		for (int i = 0; i < keys.size(); ++i)
			ps.setString(i + 1, keys.get(i));
		return ps;
	}

	public PreparedStatement prepareInsert(Connection conn, String key,
			Object value, Transcoder transcoder) throws SQLException,
			IOException {
		PreparedStatement ps = conn
				.prepareStatement(String
						.format(
								"insert into %1$s (%2$s, %3$s) values (?, ?) on duplicate key update %3$s = values(%3$s)",
								table, keyField, valueField));
		ps.setString(1, key);
		ps.setBytes(2, transcoder.encode(value));
		return ps;
	}

	public PreparedStatement prepareDelete(Connection conn, String key)
			throws SQLException, IOException {
		PreparedStatement ps = conn.prepareStatement(String.format(
				"delete from %1$s where %2$s = ?", table, keyField));
		ps.setString(1, key);
		return ps;
	}

	public PreparedStatement prepareCount(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(String.format(
				"select count(*) from %1$s", table));
		return ps;
	}

	public PreparedStatement prepareIterator(Connection conn)
			throws SQLException {
		PreparedStatement ps = conn.prepareStatement(String.format(
				"select %1$s, NULL from %2$s", keyField, table));
		return ps;
	}

	public KeyValuePair read(ResultSet rs, Transcoder transcoder)
			throws SQLException, IOException {
		String key = rs.getString(1);
		byte[] bytes = rs.getBytes(2);
		Object obj = (bytes == null) ? null : transcoder.decode(bytes);
		return new KeyValuePair(key, obj);
	}

	public void write(PreparedStatement ps, Object obj) {
	}

}
