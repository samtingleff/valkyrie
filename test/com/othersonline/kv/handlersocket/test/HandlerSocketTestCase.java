package com.othersonline.kv.handlersocket.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import junit.framework.TestCase;

import com.othersonline.kv.backends.handlersocket.HSClient;
import com.othersonline.kv.backends.handlersocket.impl.HSClientImpl;

public class HandlerSocketTestCase extends TestCase {
	private Connection conn;

	public void setUp() throws Exception {
		conn = getConnection();
		createTable(conn);
	}

	public void tearDown() throws Exception {
		dropTable(conn);
		conn.close();
	}

	public void testHs4j() throws Exception {
		String key = "test.key";
		byte[] written = new byte[] { -84, -19, 0, 5, 115, 114, 0, 61, 99, 111, 109, 46, 111, 116, 104, 101, 114, 115, 111, 110, 108, 105, 110, 101, 46, 107, 118, 46, 116, 101, 115, 116, 46, 75, 101, 121, 86, 97, 108, 117, 101, 83, 116, 111, 114, 101, 66, 97, 99, 107, 101, 110, 100, 84, 101, 115, 116, 67, 97, 115, 101, 36, 83, 97, 109, 112, 108, 101, 86, 101, 1, -37, -4, -113, -22, -12, -7, 2, 0, 3, 68, 0, 18, 115, 111, 109, 101, 79, 112, 116, 105, 111, 110, 97, 108, 68, 111, 117, 98, 108, 101, 73, 0, 15, 115, 111, 109, 101, 82, 101, 113, 117, 105, 114, 101, 100, 73, 110, 116, 76, 0, 10, 115, 111, 109, 101, 83, 116, 114, 105, 110, 103, 116, 0, 18, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 59, 120, 112, 64, 40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 116, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100 };
		byte[] read = null;

		// write over jdbc
		writeBytesJDBC(key, written);

		// read over jdbc
		read = readBytesJDBC(key);
		assertEquals(written, read);

		// read over hs4j
		read = readBytesHS4J(key);
		assertEquals(written, read);
		
		// delete over hs4j
		deleteHS4j(key);
		
		// write over hs4j
		insertBytesHS4J(key, written);
		
		// read back over jdbc
		read = readBytesJDBC(key);
		assertEquals(written, read);
	}

	public void assertEquals(byte[] b1, byte[] b2) {
		assertNotNull(b1);
		assertNotNull(b2);
		assertEquals(b1.length, b2.length);
		for (int i = 0; i < b1.length; ++i) {
			assertEquals(b1[i], b2[i]);
		}
	}

	private byte[] readBytesHS4J(String key) throws Exception {
		HSClient client = getHSClientReader();
		ResultSet rs = client.find(0, new String[] { key });
		try {
			if (rs.next()) {
				byte[] bytes = rs.getBytes(1);
				return bytes;
			} else
				return new byte[] {};
		} finally {
			rs.close();
			client.shutdown();
		}
	}
	
	private void insertBytesHS4J(String key, byte[] bytes) throws Exception {
		HSClient client = getHSClientWriter();
		try {
			client.insert(0, new byte[][] { key.getBytes(), bytes });
		} finally {
			client.shutdown();
		}
	}

	private void deleteHS4j(String key) throws Exception {
		HSClient client = getHSClientWriter();
		try {
		client.delete(0, new String[] { key });
		} finally { client.shutdown(); }
	}

	private byte[] readBytesJDBC(String key) throws Exception {
		PreparedStatement ps = conn
				.prepareStatement("select value from hs4jtest where id = ?");
		try {
			ps.setString(1, key);
			ResultSet rs = ps.executeQuery();
			assertTrue(rs.next());
			byte[] bytes = rs.getBytes(1);
			return bytes;
		} finally {
			ps.close();
		}
	}

	private void writeBytesJDBC(String key, byte[] bytes) throws Exception {
		Connection conn = getConnection();
		try {
			PreparedStatement ps = conn
			.prepareStatement("insert into hs4jtest (id, value) values (?, ?)");
			ps.setString(1, key);
			ps.setBytes(2, bytes);
			ps.execute();
		} finally {
			conn.close();
		}
	}

	private HSClient getHSClientReader() throws Exception {
		HSClient client = new HSClientImpl("otto", 9998);
		client.openIndex(0, "hs4j", "hs4jtest", "PRIMARY",
				new String[] { "value" });
		return client;		
	}

	private HSClient getHSClientWriter() throws Exception {
		HSClient client = new HSClientImpl("otto", 9999);
		client.openIndex(0, "hs4j", "hs4jtest", "PRIMARY",
				new String[] { "value" });
		return client;		
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
