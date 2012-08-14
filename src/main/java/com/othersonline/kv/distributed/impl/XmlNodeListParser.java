package com.othersonline.kv.distributed.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.othersonline.kv.distributed.ConfigurationException;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeListParser;

/**
 * Parses an xml document describing a list of nodes.
 * 
 * <node-list>
 *  <node>
 *   <id>1</id>
 *   <physical-id>1</physical-id>
 *   <salt>some-unique-semi-random-string-1</salt>
 *   <connection-uri>tyrant://host1:1978?socketTimeout=200&maxActive=5</connection-uri>
 *  </node>
 *  <node>
 *   <id>2</id>
 *   <physical-id>1</physical-id>
 *   <salt>some-unique-semi-random-string-2</salt>
 *   <connection-uri>tyrant://host1:1979?socketTimeout=200&maxActive=5</connection-uri>
 *  </node>
 *  <node>
 *   <id>3</id>
 *   <physical-id>2</physical-id>
 *   <salt>some-unique-semi-random-string-3</salt>
 *   <connection-uri>tyrant://host2:1978?socketTimeout=200&maxActive=5</connection-uri>
 *  </node>
 * </node-list>
 * 
 * @author sam
 * 
 */
public class XmlNodeListParser implements NodeListParser {

	public List<Node> parse(InputStream is) throws IOException,
			ConfigurationException {
		try {
			SAXBuilder saxBuilder = new SAXBuilder();
			Document doc = saxBuilder.build(is);
			Element root = doc.getRootElement();
			List<Element> nodeElementList = root.getChildren("node");
			List<Node> result = new ArrayList<Node>(nodeElementList.size());
			for (Element nodeElement : nodeElementList) {
				Node node = parse(nodeElement);
				result.add(node);
			}
			return result;
		} catch (JDOMException e) {
			throw new ConfigurationException(e);
		} finally {
		}
	}

	private Node parse(Element el) {
		DefaultNodeImpl node = new DefaultNodeImpl();
		node.setConnectionURI(el.getChildText("connection-uri"));
		node.setId(Integer.parseInt(el.getChildText("id")));
		node.setPhysicalId(Integer.parseInt(el.getChildText("physical-id")));
		node.setSalt(el.getChildText("salt"));
		return node;
	}
}
