package com.othersonline.kv.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.backends.IterableKeyValueStore;
import com.othersonline.kv.backends.UriConnectionFactory;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.impl.DistributedKeyValueStoreClientImpl;
import com.othersonline.kv.distributed.impl.PropertiesConfigurator;
import com.othersonline.kv.transcoder.ByteArrayTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

/**
 * Valkyrie rebalancing job. Given an input node (by uri) and a destination valkryie store, will:
 *  1) Write data that does NOT belong on the input node to the output store
 *  2) Delete successful writes from above on the input node
 * 
 * @author stingleff
 *
 */
public class ValkyrieRebalance implements Runnable, Callable<Map<String, Long>> {

	@Option(name = "--source", usage = "Source uri")
	private String source;

	@Option(name = "--node", usage = "Source node id")
	private int nodeId;

	@Option(name = "--properties", usage = "Properties file for destination valkyrie client")
	private String properties;

	private Transcoder byteTranscoder = new ByteArrayTranscoder();

	private Properties props;

	public static void main(String[] args) throws Exception {
		ValkyrieRebalance vr = new ValkyrieRebalance();
		CmdLineParser parser = new CmdLineParser(vr);
		parser.parseArgument(args);
		Map<String, Long> stats = vr.call();
		for (Map.Entry<String, Long> entry : stats.entrySet()) {
			System.out.println(String.format("%1$s\t%2$d", entry.getKey(), entry.getValue()));
		}
	}

	public void run() {
		try {
			call();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Map<String, Long> call() throws Exception {
		IterableKeyValueStore src = getSource();
		DistributedKeyValueStoreClientImpl valkyrie = getDestination();

		long moved = 0, unmoved = 0;
		int writeReplicas = Integer.parseInt(props.getProperty("write.replicas"));
		Iterator<String> iter = src.iterkeys().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			List<Node> preferenceList = valkyrie.getPreferenceList(key, writeReplicas);
			boolean set = true;
			for (Node node : preferenceList) {
				if (node.getId() == nodeId) {
					set = false;
					break;
				}
			}
			if (set) {
				try {
					byte[] bytes = (byte[]) src.get(key, byteTranscoder);
					valkyrie.set(key, bytes, byteTranscoder);
					src.delete(key);
					++moved;
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else
				++unmoved;
		}
		Map<String, Long> results = new HashMap<String, Long>();
		results.put("moved", moved);
		results.put("unmoved", unmoved);
		return results;
	}

	private IterableKeyValueStore getSource() throws KeyValueStoreUnavailable,
			IOException {
		UriConnectionFactory factory = new UriConnectionFactory();
		KeyValueStore src = factory.getStore(source);
		if (!(src instanceof IterableKeyValueStore)) {
			throw new IllegalArgumentException(
					String
							.format(
									"Source must implement the IterableKeyValueStore interface. %1$s from %2$s does not.",
									src, source));
		} else
			return (IterableKeyValueStore) src;
	}

	private DistributedKeyValueStoreClientImpl getDestination()
			throws FileNotFoundException, IOException {
		PropertiesConfigurator configurator = new PropertiesConfigurator();
		this.props = getProperties();
		configurator.load(props);
		DistributedKeyValueStoreClientImpl dest = new DistributedKeyValueStoreClientImpl();
		dest.setConfigurator(configurator);
		dest.start();
		return dest;
	}

	private Properties getProperties() throws IOException {
		Properties props = new Properties();
		File file = new File(properties);
		FileInputStream fis = new FileInputStream(file);
		try {
			props.load(fis);
		} finally {
			fis.close();
		}
		return props;
	}
}
