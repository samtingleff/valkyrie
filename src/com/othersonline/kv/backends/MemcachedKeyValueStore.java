package com.othersonline.kv.backends;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.KetamaNodeLocator;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.mgmt.BaseKeyValueStoreImplMXBean;
import com.othersonline.kv.mgmt.KeyValueStoreMXBean;
import com.othersonline.kv.mgmt.MemcachedImplMXBean;
import com.othersonline.kv.transcoder.Transcoder;

/**
 * Proxy to the spy memcached client. Comments are copied from javadoc for that
 * client.
 * 
 * @author sam
 * 
 */
public class MemcachedKeyValueStore extends BaseManagedKeyValueStore implements
		KeyValueStore {
	public static final String IDENTIFIER = "memcached";

	public MemcachedClient mcc;

	private boolean useBinaryProtocol = false;

	private boolean useKetama = true;

	private List<InetSocketAddress> hosts;

	public MemcachedKeyValueStore() {
	}

	public MemcachedKeyValueStore(String hosts) {
		setHosts(hosts);
	}

	public MemcachedKeyValueStore(List<InetSocketAddress> hosts) {
		this.hosts = hosts;
	}

	public void setHosts(String hosts) {
		this.hosts = AddrUtil.getAddresses(hosts);
	}

	public void setUseBinaryProtocol(boolean useBinaryProtocol) {
		this.useBinaryProtocol = useBinaryProtocol;
	}

	public void setUseKetama(boolean useKetama) {
		this.useKetama = useKetama;
	}

	@Override
	public void start() throws IOException {
		ConnectionFactory cf = null;
		if (useBinaryProtocol && useKetama)
			cf = new KetamaBinaryConnectionFactory();
		else if (useBinaryProtocol)
			cf = new BinaryConnectionFactory();
		else if (useKetama)
			cf = new KetamaConnectionFactory();
		else
			cf = new DefaultConnectionFactory();
		mcc = new MemcachedClient(cf, hosts);
		super.start();
	}

	@Override
	public void stop() {
		mcc.shutdown();
		mcc = null;
		super.stop();
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		boolean value = (mcc.get(key) != null);
		return value;
	}

	@Override
	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		Object value = mcc.get(key);
		return value;
	}

	@Override
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return mcc.get(key);
	}

	@Override
	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		mcc.set(key, 0, value);
	}

	@Override
	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		mcc.set(key, 0, value);
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		mcc.delete(key);
	}

	/**
	 * Increment the given counter, returning the new value.
	 * 
	 * @param key
	 *            the key
	 * @param by
	 *            the amount to increment
	 * @param def
	 *            the default value (if the counter does not exist)
	 * @return the new value, or -1 if we were unable to increment or add
	 */
	public long incr(String key, int by, long def) {
		return mcc.incr(key, by, def);
	}

	/**
	 * Increment the given counter, returning the new value.
	 * 
	 * @param key
	 *            the key
	 * @param by
	 *            the amount to increment
	 * @param def
	 *            the default value (if the counter does not exist)
	 * @param exp
	 *            the expiration of this object
	 * @return the new value, or -1 if we were unable to increment or add
	 */
	public long incr(String key, int by, long def, int exp) {
		return mcc.incr(key, by, def, exp);
	}

	/**
	 * Decrement the given counter, returning the new value.
	 * 
	 * @param key
	 *            the key
	 * @param by
	 *            the amount to decrement
	 * @param def
	 *            the default value (if the counter does not exist)
	 * @return the new value, or -1 if we were unable to decrement or add
	 */
	public long decr(String key, int by, long def) {
		return mcc.decr(key, by, def);
	}

	/**
	 * Decrement the given counter, returning the new value.
	 * 
	 * @param key
	 *            the key
	 * @param by
	 *            the amount to decrement
	 * @param def
	 *            the default value (if the counter does not exist)
	 * @param exp
	 *            the expiration of this object
	 * @return the new value, or -1 if we were unable to decrement or add
	 */
	public long decr(String key, int by, long def, int exp) {
		return mcc.incr(key, by, def, exp);
	}

	/**
	 * Get all of the stats from all of the connections.
	 * 
	 * @return Map of all stats from all hosts
	 */
	public Map<SocketAddress, Map<String, String>> getStats() {
		return mcc.getStats();
	}

	/**
	 * Get a set of stats from all connections.
	 * 
	 * @param arg
	 *            which stats to get
	 * @return map of matching stats from all hosts
	 */
	public Map<SocketAddress, Map<String, String>> getStats(String arg) {
		return mcc.getStats(arg);
	}

	/**
	 * Get the addresses of unavailable servers.
	 * 
	 * This is based on a snapshot in time so shouldn't be considered completely
	 * accurate, but is a useful for getting a feel for what's working and
	 * what's not working.
	 * 
	 * @return collection of currently unavailable servers
	 */
	public Collection<SocketAddress> getUnavailableServers() {
		return mcc.getUnavailableServers();
	}

	/**
	 * Get the versions of all of the connected memcacheds.
	 * 
	 * @return map of server version on all hosts
	 */
	public Map<SocketAddress, String> getVersions() {
		return mcc.getVersions();
	}

	public Object getMXBean() {
		return new MemcachedImplMXBean(this);
	}

	/**
	 * A binary wire protocol that uses ketama hashing and ketama node locator.
	 * 
	 * @author sam
	 * 
	 */
	private static class KetamaBinaryConnectionFactory extends
			DefaultConnectionFactory {

		public KetamaBinaryConnectionFactory(int qLen, int bufSize) {
			super(qLen, bufSize, HashAlgorithm.KETAMA_HASH);
		}

		public KetamaBinaryConnectionFactory() {
			this(DEFAULT_OP_QUEUE_LEN, DEFAULT_READ_BUFFER_SIZE);
		}

		@Override
		public NodeLocator createLocator(List<MemcachedNode> nodes) {
			return new KetamaNodeLocator(nodes, getHashAlg());
		}

		@Override
		public MemcachedNode createMemcachedNode(SocketAddress sa,
				SocketChannel c, int bufSize) {
			return new BinaryMemcachedNodeImpl(sa, c, bufSize,
					createReadOperationQueue(), createWriteOperationQueue(),
					createOperationQueue());
		}

		@Override
		public OperationFactory getOperationFactory() {
			return new BinaryOperationFactory();
		}
	}
}
