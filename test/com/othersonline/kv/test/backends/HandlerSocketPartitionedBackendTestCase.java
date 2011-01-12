package com.othersonline.kv.test.backends;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import com.othersonline.kv.backends.HandlerSocketPartitionedKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;

public class HandlerSocketPartitionedBackendTestCase extends
		KeyValueStoreBackendTestCase {
	private Connection conn;

	public void setUp() throws Exception {
		conn = getConnection();
		createTables(conn);
	}

	public void tearDown() throws Exception {
		dropTables(conn);
		conn.close();
	}

	@Override
	public void testBackend() throws Exception {
		HandlerSocketPartitionedKeyValueStore kv = new HandlerSocketPartitionedKeyValueStore();
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

	private void dropTables(Connection conn) throws Exception {
		try {
			for (int i = 0; i < 16; ++i) {
				String table = String.format("hs4jtest_%03d", i);
				PreparedStatement ps = conn.prepareStatement("drop table "
						+ table);
				ps.execute();
				ps.close();
			}
		} finally {
		}
	}

	private void createTables(Connection conn) throws Exception {
		try {
			for (int i = 0; i < 16; ++i) {
				String table = String.format("hs4jtest_%03d", i);
				PreparedStatement ps = conn
						.prepareStatement("create table "
								+ table
								+ " ( id varchar(32) primary key not null, value blob ) ENGINE=InnoDB DEFAULT CHARSET=latin1");
				ps.execute();
				ps.close();
			}
		} finally {
		}
	}

	private Connection getConnection() throws Exception {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection("jdbc:mysql://otto/hs4j",
				"hs4j", "hs4j");
		return conn;
	}
}
