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
import com.othersonline.kv.backends.TokyoTyrantKeyValueStore;
import com.othersonline.kv.distributed.ConnectionFactory;
import com.othersonline.kv.distributed.Node;

public class TokyoTyrantConnectionFactory extends AbstractConnectionFactory
		implements ConnectionFactory {
	private Log log = LogFactory.getLog(getClass());

	// urls are specified as tcp://hostname:port
	private Pattern urlPattern = Pattern
			.compile("tcp:\\/\\/([\\w]+):([0-9]+)(\\?(.*))?");

	protected KeyValueStore createStoreConnection(Node node)
			throws IOException, KeyValueStoreUnavailable {
		Matcher m = urlPattern.matcher(node.getConnectionURI());
		if (!m.matches())
			throw new IllegalArgumentException(String.format(
					"The url pattern %1$s does not match tcp://hostname:port",
					node.getConnectionURI()));

		String host = m.group(1);
		int port = Integer.parseInt(m.group(2));
		KeyValueStore store = new TokyoTyrantKeyValueStore(host, port);

		try {
			Map<String, String> configs = new HashMap<String, String>();
			if (m.group(4) != null) {
				String[] args = m.group(4).split("&");
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
}
