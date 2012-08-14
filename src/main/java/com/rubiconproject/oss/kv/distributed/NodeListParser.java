package com.rubiconproject.oss.kv.distributed;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface NodeListParser {

	public List<Node> parse(InputStream is) throws IOException,
			ConfigurationException;
}
