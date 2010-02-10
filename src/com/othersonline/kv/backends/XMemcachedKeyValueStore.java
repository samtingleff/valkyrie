package com.othersonline.kv.backends;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.impl.KetamaMemcachedSessionLocator;
import net.rubyeye.xmemcached.utils.AddrUtil;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.mgmt.XMemcachedImplMXBean;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.transcoder.xmemcached.XMemcachedByteArrayTranscoder;

/**
 * Proxy to the xmemcached client.
 * 
 * @author sam
 * 
 */
public class XMemcachedKeyValueStore extends BaseManagedKeyValueStore implements
		KeyValueStore {
	public static final String IDENTIFIER = "xmemcached";

	private XMemcachedByteArrayTranscoder xmemcachedTranscoder = new XMemcachedByteArrayTranscoder();

	private MemcachedClient mcc;

	private boolean useBinaryProtocol = false;

	private boolean useKetama = true;

	private long getOperationTimeout = 1000l;

	private long setOperationTimeout = 1000l;

	private List<InetSocketAddress> hosts;

	private String host = "localhost";

	private int port = 11211;

	public XMemcachedKeyValueStore() {
	}

	public XMemcachedKeyValueStore(String hosts) {
		setHosts(hosts);
	}

	public XMemcachedKeyValueStore(List<InetSocketAddress> hosts) {
		this.hosts = hosts;
	}

	@Configurable(name = "host", accepts = Type.StringType)
	public void setHost(String host) {
		this.host = host;
	}

	@Configurable(name = "port", accepts = Type.IntType)
	public void setPort(int port) {
		this.port = port;
	}

	@Configurable(name = "hosts", accepts = Type.StringType)
	public void setHosts(String hosts) {
		this.hosts = AddrUtil.getAddresses(hosts);
	}

	@Configurable(name = "useBinaryProtocol", accepts = Type.BooleanType)
	public void setUseBinaryProtocol(boolean useBinaryProtocol) {
		this.useBinaryProtocol = useBinaryProtocol;
	}

	@Configurable(name = "useKetama", accepts = Type.BooleanType)
	public void setUseKetama(boolean useKetama) {
		this.useKetama = useKetama;
	}

	@Configurable(name = "getOperationTimeout", accepts = Type.LongType)
	public void setGetOperationTimeout(long millis) {
		this.getOperationTimeout = millis;
	}

	@Configurable(name = "setOperationTimeout", accepts = Type.LongType)
	public void setSetOperationTimeout(long millis) {
		this.setOperationTimeout = millis;
	}

	public void start() throws IOException {
		List<InetSocketAddress> hostList = (hosts == null) ? Arrays
				.asList(new InetSocketAddress(host, port)) : hosts;
		XMemcachedClientBuilder builder = new XMemcachedClientBuilder(hostList);
		if (useBinaryProtocol)
			builder.setCommandFactory(new BinaryCommandFactory());
		if (useKetama)
			builder.setSessionLocator(new KetamaMemcachedSessionLocator());
		this.mcc = builder.build();
		super.start();
	}

	public void stop() {
		try {
			mcc.shutdown();
		} catch (IOException e) {
		}
		mcc = null;
		super.stop();
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		MemcachedClient mcc = getMemcachedClient();
		try {
			boolean value = (mcc.get(key) != null);
			return value;
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		MemcachedClient mcc = getMemcachedClient();
		try {
			Object value = (getOperationTimeout > 0) ? mcc.get(key, getOperationTimeout) : mcc.get(key);
			return value;
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		MemcachedClient mcc = getMemcachedClient();
		try {
			byte[] bytes = (getOperationTimeout > 0) ? mcc.get(key, getOperationTimeout, xmemcachedTranscoder) : mcc.get(key, xmemcachedTranscoder);
			if (bytes == null)
				return null;
			else {
				Object obj = transcoder.decode(bytes);
				return obj;
			}
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		MemcachedClient mcc = getMemcachedClient();
		try {
			Map<String, Object> results = (getOperationTimeout > 0) ? mcc.get(Arrays.asList(keys), getOperationTimeout) : mcc.get(Arrays.asList(keys));
			return results;
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	public Map<String, Object> getBulk(final List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		MemcachedClient mcc = getMemcachedClient();
		try {
			Map<String, Object> results = (getOperationTimeout > 0) ? mcc.get(keys, getOperationTimeout) : mcc.get(keys);
			return results;
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	public Map<String, Object> getBulk(final List<String> keys,
			Transcoder transcoder) throws KeyValueStoreException, IOException {
		assertReadable();
		MemcachedClient mcc = getMemcachedClient();
		try {
			Map<String, byte[]> results = (getOperationTimeout > 0) ? mcc.get(keys, getOperationTimeout, xmemcachedTranscoder) : mcc.get(keys, xmemcachedTranscoder);
			Map<String, Object> retval = new HashMap<String, Object>(results
					.size());
			for (Entry<String, byte[]> entry : results.entrySet()) {
				byte[] bytes = entry.getValue();
				Object obj = transcoder.decode(bytes);
				retval.put(entry.getKey(), obj);
			}
			return retval;
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		set(key, value, 0);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		set(key, value, transcoder, 0);
	}

	public void set(String key, Object value, int exp)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		MemcachedClient mcc = getMemcachedClient();
		try {
			if (setOperationTimeout > 0)
				mcc.set(key, exp, value, setOperationTimeout);
			else
				mcc.set(key, exp, value);
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	public void set(String key, Object value, Transcoder transcoder, int exp)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		MemcachedClient mcc = getMemcachedClient();
		try {
			byte[] bytes = transcoder.encode(value);
			if (setOperationTimeout > 0)
			mcc.set(key, exp, bytes, xmemcachedTranscoder, setOperationTimeout);
			else
				mcc.set(key, exp, bytes, xmemcachedTranscoder);
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		MemcachedClient mcc = getMemcachedClient();
		try {
			mcc.delete(key);
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
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
	 * @throws KeyValueStoreException
	 */
	public long incr(String key, int by, long def)
			throws KeyValueStoreException {
		MemcachedClient mcc = getMemcachedClient();
		try {
			return (setOperationTimeout > 0) ? mcc.incr(key, by, def, setOperationTimeout) : mcc.incr(key, by, def);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
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
	 * @throws KeyValueStoreException
	 */
	public long decr(String key, int by, long def)
			throws KeyValueStoreException {
		MemcachedClient mcc = getMemcachedClient();
		try {
			return (setOperationTimeout > 0) ? mcc.decr(key, by, def, setOperationTimeout) : mcc.incr(key, by, def);
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	/**
	 * Get all of the stats from all of the connections.
	 * 
	 * @return Map of all stats from all hosts
	 * @throws KeyValueStoreException
	 */
	public Map<InetSocketAddress, Map<String, String>> getStats()
			throws KeyValueStoreException {
		MemcachedClient mcc = getMemcachedClient();
		try {
			return mcc.getStats();
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	/**
	 * Get the addresses of unavailable servers.
	 * 
	 * This is based on a snapshot in time so shouldn't be considered completely
	 * accurate, but is a useful for getting a feel for what's working and
	 * what's not working.
	 * 
	 * @return collection of currently unavailable servers
	 * @throws KeyValueStoreException
	 */
	public Collection<InetSocketAddress> getUnavailableServers()
			throws KeyValueStoreException {
		MemcachedClient mcc = getMemcachedClient();
		try {
			return mcc.getAvaliableServers();
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	/**
	 * Get the versions of all of the connected memcacheds.
	 * 
	 * @return map of server version on all hosts
	 * @throws KeyValueStoreException
	 */
	public Map<InetSocketAddress, String> getVersions()
			throws KeyValueStoreException {
		MemcachedClient mcc = getMemcachedClient();
		try {
			return mcc.getVersions();
		} catch (TimeoutException e) {
			throw new KeyValueStoreException(e);
		} catch (InterruptedException e) {
			throw new KeyValueStoreException(e);
		} catch (MemcachedException e) {
			throw new KeyValueStoreException(e);
		} finally {
			releaseMemcachedClient(mcc);
		}
	}

	public Object getMXBean() {
		return new XMemcachedImplMXBean(this);
	}

	private MemcachedClient getMemcachedClient() throws KeyValueStoreException {
		try {
			return mcc;
		} catch (Exception e) {
			throw new KeyValueStoreException(e);
		}
	}

	private void releaseMemcachedClient(MemcachedClient client) {
	}
}
