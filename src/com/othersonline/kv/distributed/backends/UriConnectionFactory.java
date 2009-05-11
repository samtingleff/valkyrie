package com.othersonline.kv.distributed.backends;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.backends.BDBJEKeyValueStore;
import com.othersonline.kv.backends.ConcurrentHashMapKeyValueStore;
import com.othersonline.kv.backends.FileSystemKeyValueStore;
import com.othersonline.kv.backends.KosmosfsKeyValueStore;
import com.othersonline.kv.backends.MemcachedKeyValueStore;
import com.othersonline.kv.backends.ThriftKeyValueStore;
import com.othersonline.kv.backends.TokyoTyrantKeyValueStore;
import com.othersonline.kv.backends.VoldemortKeyValueStore;
import com.othersonline.kv.backends.WebDAVKeyValueStore;
import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Node;

public class UriConnectionFactory extends AbstractConnectionFactory implements
		ConnectionFactory {
	private Log log = LogFactory.getLog(getClass());

	// urls are specified as schema://hostname:port?arg1=value1&arg2=value2
	// where :port and ?arg1... are optional
	private Pattern urlPattern = Pattern
			.compile("([\\w]+):\\/\\/([\\w]+)(:([0-9]+))?(\\?(.*))?");

	protected KeyValueStore createStoreConnection(Node node)
			throws IOException, KeyValueStoreUnavailable {
		Matcher m = urlPattern.matcher(node.getConnectionURI());
		if (!m.matches())
			throw new IllegalArgumentException(
					String
							.format(
									"The url pattern %1$s does not match type://hostname:port?args...",
									node.getConnectionURI()));

		String type = m.group(1);

		KeyValueStore store = getStore(type);

		try {
			Map<String, String> configs = new HashMap<String, String>();
			if (m.group(2) != null) {
				configs.put("host", m.group(2));
			}
			if (m.group(4) != null) {
				configs.put("port", m.group(4));
			}
			if (m.group(6) != null) {
				String[] args = m.group(6).split("&");
				for (String arg : args) {
					String[] values = arg.split("=");
					if (values.length == 2) {
						configs.put(values[0], values[1]);
					}
				}
				super.configureStore(store, configs);
			}
		} catch (Exception e) {
			log.warn("Error parsing connection string:", e);
		}
		return store;
	}

	private KeyValueStore getStore(String type) {
		KeyValueStore store = null;
		if ("bdbje".equals(type))
			store = new BDBJEKeyValueStore();
		else if ("hash".equals(type))
			store = new ConcurrentHashMapKeyValueStore();
		else if ("fs".equals(type))
			store = new FileSystemKeyValueStore();
		else if ("kfs".equals(type))
			store = new KosmosfsKeyValueStore();
		else if ("memcached".equals(type))
			store = new MemcachedKeyValueStore();
		else if ("thrift".equals(type))
			store = new ThriftKeyValueStore();
		else if ("tyrant".equals(type))
			store = new TokyoTyrantKeyValueStore();
		else if ("voldemort".equals(type))
			store = new VoldemortKeyValueStore();
		else if ("dav".equals(type))
			store = new WebDAVKeyValueStore();
		return store;
	}
}
