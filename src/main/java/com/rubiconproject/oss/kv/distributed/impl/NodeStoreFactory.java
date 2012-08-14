package com.rubiconproject.oss.kv.distributed.impl;

import java.net.MalformedURLException;
import java.util.Properties;

import com.rubiconproject.oss.kv.distributed.ConfigurationException;
import com.rubiconproject.oss.kv.distributed.NodeListParser;
import com.rubiconproject.oss.kv.distributed.NodeStore;

public class NodeStoreFactory {

	public static NodeStore getNodeStore(Properties props)
			throws ConfigurationException {
		try {
			String name = props.getProperty("nodeStore");

			NodeStore store = null;
			if ("jdbc".equals(name)) {
				store = new JdbcNodeStore(props);
			} else if ("url".equals(name)) {
				store = new UrlNodeStore(props
						.getProperty(UrlNodeStore.URL_PROPERTY),
						getNodeListParser(props));
			}
			return store;
		} catch (MalformedURLException e) {
			throw new ConfigurationException(e);
		} finally {
		}
	}

	private static NodeListParser getNodeListParser(Properties props) {
		// other, non-xml formats might go here
		return new XmlNodeListParser();
	}
}
