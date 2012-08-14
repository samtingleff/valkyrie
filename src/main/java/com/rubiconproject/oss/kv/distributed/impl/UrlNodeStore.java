package com.rubiconproject.oss.kv.distributed.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Properties;

import com.rubiconproject.oss.kv.distributed.AbstractRefreshingNodeStore;
import com.rubiconproject.oss.kv.distributed.ConfigurationException;
import com.rubiconproject.oss.kv.distributed.Node;
import com.rubiconproject.oss.kv.distributed.NodeListParser;

public class UrlNodeStore extends AbstractRefreshingNodeStore {
	public static final String URL_PROPERTY = "nodeStore.url";

	private URL url;

	private NodeListParser parser;

	public UrlNodeStore() {
		parser = new XmlNodeListParser();
	}

	public UrlNodeStore(String url) throws MalformedURLException {
		this.url = new URL(url);
		this.parser = new XmlNodeListParser();
	}

	public UrlNodeStore(String url, NodeListParser parser)
			throws MalformedURLException {
		this.url = new URL(url);
		this.parser = parser;
	}

	public void setProperties(Properties props) throws IllegalArgumentException {
		try {
			this.url = new URL(props.getProperty(URL_PROPERTY));
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public void setUrl(String url) throws MalformedURLException {
		this.url = new URL(url);
	}

	public void setParser(NodeListParser parser) {
		this.parser = parser;
	}

	@Override
	public List<Node> refreshActiveNodes() throws IOException,
			ConfigurationException {
		URLConnection conn = url.openConnection();
		InputStream in = conn.getInputStream();
		try {
			List<Node> nodes = parser.parse(in);
			return nodes;
		} finally {
			if (in != null)
				in.close();
		}
	}

}
