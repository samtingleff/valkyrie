package com.rubiconproject.oss.kv.distributed.test;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.rubiconproject.oss.kv.distributed.Node;
import com.rubiconproject.oss.kv.distributed.NodeStore;
import com.rubiconproject.oss.kv.distributed.impl.DefaultNodeImpl;
import com.rubiconproject.oss.kv.distributed.impl.JdbcNodeStore;
import com.rubiconproject.oss.kv.distributed.impl.NodeStoreFactory;

import junit.framework.TestCase;

public class NodeStoreTestCase extends TestCase {
	private static final String DATA_SOURCE_NAME = "java:comp/env/jdbc/test_db";

	public void setUp() throws Exception {
		createDataSource();
	}

	public void tearDown() throws Exception {
		cleanupNodes();
	}

	public void testJdbcNodeStore() throws Exception {
		Properties props = new Properties();
		props.setProperty("nodeStore", "jdbc");
		props.setProperty(JdbcNodeStore.DATA_SOURCE_PROPERTY, DATA_SOURCE_NAME);
		NodeStore store = NodeStoreFactory.getNodeStore(props);
		assertNotNull(store);

		List<Node> nodes = store.getActiveNodes();
		assertNotNull(nodes);
		assertEquals(nodes.size(), 0);

		// should be able to add nodes at runtime
		store.addNode(new DefaultNodeImpl(1, 1, "salt-1",
				"hash://localhost?id=1"));
		store.addNode(new DefaultNodeImpl(2, 1, "salt-2",
				"hash://localhost?id=2"));
		nodes = store.getActiveNodes();
		assertEquals(nodes.size(), 2);

		// and remove them
		store.removeNode(nodes.get(1));
		nodes = store.getActiveNodes();
		assertEquals(nodes.size(), 1);
		assertEquals(nodes.get(0).getId(), 1);

		// should be able to add that node back
		store.addNode(new DefaultNodeImpl(2, 1, "salt-2",
				"hash://localhost?id=2"));
		assertEquals(nodes.size(), 2);
		assertEquals(nodes.get(1).getId(), 2);

		// adding it again should do nothing
		store.addNode(new DefaultNodeImpl(2, 1, "salt-2",
				"hash://localhost?id=2"));
		assertEquals(nodes.size(), 2);
		assertEquals(nodes.get(1).getId(), 2);
	}

	private void cleanupNodes() throws Exception {
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup(DATA_SOURCE_NAME);
		Connection conn = ds.getConnection();
		Statement stmt = conn.createStatement();
		stmt.execute("delete from node");
		if (!conn.getAutoCommit())
			conn.commit();
		stmt.close();
		conn.close();
	}

	private void createDataSource() throws NamingException {
		Context ctx = new InitialContext();
		ctx.rebind(DATA_SOURCE_NAME, new DummyDataSource());
	}
}
