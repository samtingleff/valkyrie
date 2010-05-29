package com.othersonline.kv.backends;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreUnavailable;

public class UriConnectionFactory extends AbstractConnectionFactory implements
		ConnectionFactory {
	private Log log = LogFactory.getLog(getClass());

	// urls are specified as schema://hostname:port?arg1=value1&arg2=value2
	// where :port and ?arg1... are optional
	private Pattern urlPattern = Pattern
			.compile("([\\w\\-]+):\\/\\/([\\w\\-\\.]+)(:([0-9]+))?(\\?(.*))?");

	public Map<String, String> getStoreProperties(String uri) {
		Map<String, String> configs = new HashMap<String, String>();
		Matcher m = urlPattern.matcher(uri);
		if (!m.matches())
			throw new IllegalArgumentException(
					String
							.format(
									"The url pattern %1$s does not match type://hostname:port?args...",
									uri));

		String type = m.group(1);
		configs.put("type", type);

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
		}

		return configs;
	}

	public KeyValueStore createStoreConnection(String uri)
			throws IOException, KeyValueStoreUnavailable {
		Matcher m = urlPattern.matcher(uri);
		if (!m.matches())
			throw new IllegalArgumentException(
					String
							.format(
									"The url pattern %1$s does not match type://hostname:port?args...",
									uri));

		String type = m.group(1);
		KeyValueStore store = openConnection(type);
		return store;
	}

	private KeyValueStore openConnection(String type) {
		KeyValueStore store = null;
		if ("bdb".equals(type))
			store = new BDBKeyValueStore();
		else if ("bdbje".equals(type))
			store = new BDBJEKeyValueStore();
		else if ("cassandra".equals(type))
			store = new CassandraKeyValueStore();
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
		else if ("cabinet".equals(type))
			store = new TokyoCabinetKeyValueStore();
		else if ("voldemort".equals(type))
			store = new VoldemortKeyValueStore();
		else if ("dav".equals(type))
			store = new WebDAVKeyValueStore();
		else if ("sql".equals(type))
			store = new JdbcKeyValueStore();
		else if ("xmemcached".equals(type))
			store = new XMemcachedKeyValueStore();
		else if ("riak".equals(type))
			store = new RiakKeyValueStore();
		return store;
	}
}
