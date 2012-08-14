package com.othersonline.kv.distributed.test;

import java.io.InputStream;
import java.util.List;

import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.impl.XmlNodeListParser;

import junit.framework.TestCase;

public class NodeListParserTestCase extends TestCase {

	public void testXmlNodeListParser() throws Exception {
		XmlNodeListParser parser = new XmlNodeListParser();
		InputStream is = getClass().getResourceAsStream(
				"/com/othersonline/kv/test/resources/sample-node-list.xml");
		List<Node> nodes = parser.parse(is);
		is.close();
		assertEquals(nodes.size(), 5);
		for (int i = 0; i < nodes.size(); ++i) {
			Node n = nodes.get(i);
			assertEquals(n.getId(), i + 1);
		}
	}
}
