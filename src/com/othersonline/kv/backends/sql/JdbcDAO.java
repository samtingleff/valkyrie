package com.othersonline.kv.backends.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.othersonline.kv.transcoder.Transcoder;

public interface JdbcDAO {
	public PreparedStatement prepareExists(Connection conn, String key)
			throws SQLException;

	public PreparedStatement prepareSelect(Connection conn, String key)
			throws SQLException;

	public PreparedStatement prepareBulkSelect(Connection conn, String... keys)
			throws SQLException;

	public PreparedStatement prepareBulkSelect(Connection conn,
			List<String> keys) throws SQLException;

	public PreparedStatement prepareInsert(Connection conn, String key,
			Object value, Transcoder transcoder) throws SQLException,
			IOException;

	public PreparedStatement prepareDelete(Connection conn, String key)
			throws SQLException, IOException;

	public PreparedStatement prepareCount(Connection conn) throws SQLException;

	public PreparedStatement prepareIterator(Connection conn)
			throws SQLException;

	public KeyValuePair read(ResultSet rs, Transcoder transcoder)
			throws SQLException, IOException;

	public void write(PreparedStatement ps, Object obj);
}
