package com.othersonline.kv.distributed.impl;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.othersonline.kv.distributed.AbstractRefreshingNodeStore;
import com.othersonline.kv.distributed.ConfigurationException;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeStore;

/**
 * Reads nodes from a jdbc database. Table should look like this:
 * 
 * create table node ( id int primary key, store_id int not null, physical_id int not null, salt
 * varchar(10) unique not null, connection_uri varchar(128) not null, status
 * tinyint not null);
 * 
 * Nodes with a status other than 1 will be ignored.
 * 
 * @author sam
 * 
 */
public class JdbcNodeStore extends AbstractRefreshingNodeStore implements
		NodeStore {
	public static final String DATA_SOURCE_PROPERTY = "nodeStore.dataSource";

	public static final String JDBC_DRIVER_PROPERTY = "nodeStore.jdbcDriver";

	public static final String JDBC_URL_PROPERTY = "nodeStore.jdbcUrl";

	public static final String JDBC_USER_PROPERTY = "nodeStore.jdbcUsername";

	public static final String JDBC_PASSWORD_PROPERTY = "nodeStore.jdbcPassword";

	public static final String JDBC_STORE_ID = "nodeStore.id";

	private Properties props;

	private DataSource ds;

	private int storeId;

	public JdbcNodeStore() {
		super();
	}

	public JdbcNodeStore(Properties props) {
		super();
		setProperties(props);
	}

	public void setProperties(Properties props) {
		this.props = props;
	}

	@Override
	public void addNode(Node node) {
		Connection conn = null;
		PreparedStatement select = null;
		PreparedStatement upsert = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			// would love to use ON DUPLICATE KEY UPDATE here but for
			// compatibility with non-mysql databases I'm not going to do so.
			select = conn
					.prepareStatement("select count(id) as count from node where id = ?");
			select.setInt(1, node.getId());
			rs = select.executeQuery();
			boolean update = false;
			if (rs.next()) {
				int count = rs.getInt(1);
				update = (count > 0);
			}
			if (update) {
				upsert = conn
						.prepareStatement("update node set status = ? where id = ?");
				upsert.setInt(1, 1);
				upsert.setInt(2, node.getId());
			} else {
				upsert = conn
						.prepareStatement("insert into node (id, store_id, physical_id, salt, connection_uri, status) values (?, ?, ?, ?, ?)");
				upsert.setInt(1, node.getId());
				upsert.setInt(2, storeId);
				upsert.setInt(3, node.getPhysicalId());
				upsert.setString(4, node.getSalt());
				upsert.setString(5, node.getConnectionURI());
				upsert.setInt(6, 1);
			}
			upsert.executeUpdate();
			if (!conn.getAutoCommit())
				conn.commit();

			// only add node to in-memory structure if above code succeeded
			super.addNode(node);
		} catch (SQLException e) {
			log.error("SQLException adding node()", e);
		} catch (NamingException e) {
			log.error("NamingException adding node()", e);
		} catch (ClassNotFoundException e) {
			log.error("ClassNotFoundException adding node()", e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (select != null) {
				try {
					select.close();
				} catch (Exception e) {
				}
			}
			if (upsert != null) {
				try {
					upsert.close();
				} catch (Exception e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
	}

	@Override
	public void removeNode(Node node) {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			// remove node before any sql operations
			super.removeNode(node);

			conn = getConnection();
			ps = conn
					.prepareStatement("update node set status = ? where id = ?");
			ps.setInt(1, 2);
			ps.setInt(2, node.getId());
			ps.execute();

			if (!conn.getAutoCommit())
				conn.commit();
		} catch (SQLException e) {
			log.error("SQLException removing node()", e);
		} catch (NamingException e) {
			log.error("NamingException removing node()", e);
		} catch (ClassNotFoundException e) {
			log.error("ClassNotFoundException removing node()", e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
	}

	@Override
	public List<Node> refreshActiveNodes() throws IOException,
			ConfigurationException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			List<Node> nodes = new LinkedList<Node>();

			conn = getConnection();
			ps = conn
					.prepareStatement("select id, physical_id, salt, connection_uri from node where store_id = ? and status = 1 order by id asc");
			ps.setInt(1, storeId);
			rs = ps.executeQuery();
			while (rs.next()) {
				DefaultNodeImpl node = new DefaultNodeImpl();
				node.setConnectionURI(rs.getString("connection_uri"));
				node.setId(rs.getInt("id"));
				node.setPhysicalId(rs.getInt("physical_id"));
				node.setSalt(rs.getString("salt"));
				nodes.add(node);
			}
			return nodes;
		} catch (SQLException e) {
			throw new IOException(e);
		} catch (NamingException e) {
			throw new ConfigurationException(e);
		} catch (ClassNotFoundException e) {
			throw new ConfigurationException(e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
	}

	private Connection getConnection() throws SQLException, NamingException,
			ClassNotFoundException {
		if (ds == null) {
			storeId = Integer.parseInt(props.getProperty(JDBC_STORE_ID));
			String dataSourceName = props.getProperty(DATA_SOURCE_PROPERTY);
			if (dataSourceName != null) {
				Context initCtx = new InitialContext();
				ds = (DataSource) initCtx.lookup(dataSourceName);
			} else {
				String driver = props.getProperty(JDBC_DRIVER_PROPERTY);
				String url = props.getProperty(JDBC_URL_PROPERTY);
				String user = props.getProperty(JDBC_USER_PROPERTY);
				String password = props.getProperty(JDBC_PASSWORD_PROPERTY);
				ds = new SimpleDataSource(driver, url, user, password);
			}
		}
		return ds.getConnection();
	}

	private class SimpleDataSource implements DataSource {
		private PrintWriter pw;

		private int loginTimeout;

		private String url;

		private String user;

		private String password;

		public SimpleDataSource(String driver, String url, String user,
				String password) throws ClassNotFoundException {
			this.url = url;
			this.user = user;
			this.password = password;
			Class.forName(driver);
		}

		public Connection getConnection() throws SQLException {
			return DriverManager.getConnection(url, user, password);
		}

		public Connection getConnection(String username, String password)
				throws SQLException {
			return getConnection();
		}

		public PrintWriter getLogWriter() throws SQLException {
			return pw;
		}

		public int getLoginTimeout() throws SQLException {
			return loginTimeout;
		}

		public void setLogWriter(PrintWriter pw) throws SQLException {
			this.pw = pw;
		}

		public void setLoginTimeout(int loginTimeout) throws SQLException {
			this.loginTimeout = loginTimeout;
		}

		public boolean isWrapperFor(Class<?> cls) throws SQLException {
			return false;
		}

		public <T> T unwrap(Class<T> cls) throws SQLException {
			return null;
		}

	}
}
