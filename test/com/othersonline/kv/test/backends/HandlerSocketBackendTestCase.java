package com.othersonline.kv.test.backends;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import com.othersonline.kv.backends.HandlerSocketKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class HandlerSocketBackendTestCase extends KeyValueStoreBackendTestCase {
	private Connection conn;

	public void setUp() throws Exception {
		conn = getConnection();
		createTable(conn);
	}

	public void tearDown() throws Exception {
		// dropTable(conn);
		conn.close();
	}

	@Override
	public void testBackend() throws Exception {
		HandlerSocketKeyValueStore kv = new HandlerSocketKeyValueStore();
		kv.setHost("otto");
		kv.setReadPort(9998);
		kv.setWritePort(9999);
		kv.setDb("hs4j");
		kv.setTable("hs4jtest");
		kv.setValueColumn("value");
		kv.setUpdateBeforeInsert(false);
		doTestBackend(kv);
		kv.stop();
	}

	private void dropTable(Connection conn) throws Exception {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement("drop table hs4jtest");
			ps.execute();
		} finally {
			ps.close();
		}
	}

	private void createTable(Connection conn) throws Exception {
		PreparedStatement ps = null;
		try {
			ps = conn
					.prepareStatement("create table hs4jtest ( id varchar(32) primary key not null, value blob ) ENGINE=InnoDB DEFAULT CHARSET=latin1");
			ps.execute();
		} finally {
			ps.close();
		}
	}

	private Connection getConnection() throws Exception {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection("jdbc:mysql://otto/hs4j",
				"hs4j", "hs4j");
		return conn;
	}
}
