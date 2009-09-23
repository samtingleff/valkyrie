package com.othersonline.kv.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.kohsuke.args4j.Option;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.backends.IterableKeyValueStore;
import com.othersonline.kv.backends.UriConnectionFactory;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.impl.DistributedKeyValueStoreClientImpl;
import com.othersonline.kv.distributed.impl.PropertiesConfigurator;
import com.othersonline.kv.transcoder.ByteArrayTranscoder;
import com.othersonline.kv.transcoder.ByteTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class ValkyrieRebalance implements Runnable, Callable<Long> {

	@Option(name = "source", usage = "Source uri")
	private String source;

	@Option(name = "node", usage = "Source node id")
	private int nodeId;

	@Option(name = "destination", usage = "Properties file for destination valkyrie client")
	private String destination;

	private Transcoder byteTranscoder = new ByteArrayTranscoder();

	public static void main(String[] args) throws Exception {
		ValkyrieRebalance vr = new ValkyrieRebalance();
		Long count = vr.call();
		System.out.println(String.format("Moved %1$d values", count));
	}

	public void run() {
		try {
			call();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Long call() throws Exception {
		IterableKeyValueStore src = getSource();
		DistributedKeyValueStoreClientImpl valkyrie = getDestination();

		long moved = 0;
		Iterator<String> iter = src.iterkeys().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			List<Node> preferenceList = valkyrie.getPreferenceList(key, 2);
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
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return moved;
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
		configurator.load(new FileInputStream(new File(destination)));
		DistributedKeyValueStoreClientImpl dest = new DistributedKeyValueStoreClientImpl();
		dest.setConfigurator(configurator);
		dest.start();
		return dest;
	}
}
