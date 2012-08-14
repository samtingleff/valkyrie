package com.rubiconproject.oss.kv.backends.sql;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

public class SimpleDataSource implements DataSource {

	private String url;

	private String username;

	private String password;

	private PrintWriter logWriter;

	private int loginTimeout;

	public SimpleDataSource(String url, String username, String password) {
		this.url = url;
		this.username = username;
		this.password = password;
	}

	public Connection getConnection() throws SQLException {
		return getConnection(this.username, this.password);
	}

	public Connection getConnection(String username, String password)
			throws SQLException {
		Connection conn = DriverManager.getConnection(this.url, username,
				password);
		return conn;
	}

	public PrintWriter getLogWriter() throws SQLException {
		return logWriter;
	}

	public int getLoginTimeout() throws SQLException {
		return loginTimeout;
	}

	public void setLogWriter(PrintWriter pw) throws SQLException {
		this.logWriter = pw;
	}

	public void setLoginTimeout(int timeout) throws SQLException {
		this.loginTimeout = timeout;
	}

	public boolean isWrapperFor(Class<?> cls) throws SQLException {
		return false;
	}

	public <T> T unwrap(Class<T> cls) throws SQLException {
		return null;
	}

}