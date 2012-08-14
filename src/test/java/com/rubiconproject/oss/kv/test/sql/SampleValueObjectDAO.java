package com.rubiconproject.oss.kv.test.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.rubiconproject.oss.kv.backends.sql.DefaultJdbcDAO;
import com.rubiconproject.oss.kv.backends.sql.JdbcDAO;
import com.rubiconproject.oss.kv.backends.sql.KeyValuePair;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

/**
 * create table sample_value_objects (id varchar(12) not null primary key, x int
 * not null, y int not null, s varchar(255) not null);
 * 
 * @author stingleff
 * 
 */
public class SampleValueObjectDAO extends DefaultJdbcDAO implements JdbcDAO {

	private static String table = "sample_value_objects";

	private static String keyField = "id";

	private static String valueField = "s";

	public SampleValueObjectDAO() {
		super(table, keyField, valueField);
	}

	public PreparedStatement prepareSelect(Connection conn, String key)
			throws SQLException {
		PreparedStatement ps = conn
				.prepareStatement(String.format(
						"select id, x, y, s from %1$s where %2$s = ?", table,
						keyField));
		ps.setString(1, key);
		return ps;
	}

	public PreparedStatement prepareBulkSelect(Connection conn, String... keys)
			throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("select id, x, y, s from %1$s where ");
		for (int i = 0; i < keys.length; ++i) {
			if (i > 0)
				sb.append(" or ");
			sb.append("id = ?");
		}
		String query = String.format(sb.toString(), table);
		PreparedStatement ps = conn.prepareStatement(query);
		for (int i = 0; i < keys.length; ++i)
			ps.setString(i + 1, keys[i]);
		return ps;
	}

	public PreparedStatement prepareBulkSelect(Connection conn,
			List<String> keys) throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("select id, x, y, s from %1$s where ");
		for (int i = 0; i < keys.size(); ++i) {
			if (i > 0)
				sb.append(" or ");
			sb.append("id = ?");
		}
		String query = String.format(sb.toString(), table);
		PreparedStatement ps = conn.prepareStatement(query);
		for (int i = 0; i < keys.size(); ++i)
			ps.setString(i + 1, keys.get(i));
		return ps;
	}

	public PreparedStatement prepareInsert(Connection conn, String key,
			Object value, Transcoder transcoder) throws SQLException,
			IOException {
		SampleValueObject svo = (SampleValueObject) value;
		PreparedStatement ps = conn
				.prepareStatement(String
						.format(
								"insert into %1$s (id, x, y, s) values (?, ?, ?, ?) on duplicate key update x = values(x), y = values(y), s = values(s)",
								table));
		ps.setString(1, key);
		ps.setInt(2, svo.getX());
		ps.setInt(3, svo.getY());
		ps.setString(4, svo.getS());
		return ps;
	}

	public KeyValuePair read(ResultSet rs, Transcoder transcoder)
			throws SQLException, IOException {
		SampleValueObject svo = new SampleValueObject(rs.getString(1), rs
				.getInt(2), rs.getInt(3), rs.getString(4));
		return new KeyValuePair(svo.getK(), svo);
	}

}
