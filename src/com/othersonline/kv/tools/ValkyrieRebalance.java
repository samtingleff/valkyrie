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
import com.othersonline.kv.backends.KeyValueStoreIterator;
import com.othersonline.kv.backends.UriConnectionFactory;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.impl.DistributedKeyValueStoreClientImpl;
import com.othersonline.kv.distributed.impl.PropertiesConfigurator;
import com.othersonline.kv.transcoder.ByteArrayTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

/**
 * Valkyrie rebalancing job.
 * 
 * Given an input node (by uri) and a destination valkryie store, will:
 *  1) Write data that does NOT belong on the input node to the output store
 *  2) Delete successful writes from above on the input node
 * 
 * Usage:
 *  - create a valkyrie properties file (in, say /tmp/valkyrie.properties) like
 *   nodestore.implementation=com.othersonline.kv.distributed.impl.JdbcNodeStore
 *   nodeStore.jdbcDriver=com.mysql.jdbc.Driver
 *   nodeStore.jdbcUrl=jdbc:mysql://dev-db/oz_central
 *   nodeStore.jdbcUsername=username
 *   nodeStore.jdbcPassword=s3cret
 *   nodeStore.id=1
 *   read.timeout = 5000
 *   read.replicas = 3
 *   read.required = 1
 *   write.timeout = 10000
 *   write.replicas = 3
 *   write.required = 3
 *   backfill.nullGets = false
 *   backfill.failedGets = false
 * 
 *  - Find the node id of the source node (say 12)
 * 
 *  - Run it:
 *   java -classpath oo-kv-storage.jar:... com.othersonline.kv.tools.ValkyrieRebalance --source "tyrant://dev-db:1978" --node 12 --sleep 20 --properties /tmp/valkyrie.properties
 * 
 * @author stingleff
 *
 */
public class ValkyrieRebalance implements Runnable, Callable<Map<String, Long>> {

	@Option(name = "--source", usage = "Source uri (default: none)", required = true)
	private String source;

	@Option(name = "--node", usage = "Source node id (default: none)", required = true)
	private int nodeId;

	@Option(name = "--properties", usage = "Properties file for destination valkyrie client (default: none)", required = true)
	private String properties;

	@Option(name = "--sleep", usage = "Sleep time between keys (millis) (default: disabled)")
	private long sleep = 0;

	@Option(name = "--skip", usage = "Skip this many records first (default: 0)")
	private int skip = 0;

	@Option(name = "--max", usage = "Stop after x keys (default: disabled)")
	private int max = -1;

	@Option(name = "--delete", usage = "Delete from source (default: false")
	private boolean delete = false;

	private Transcoder byteTranscoder = new ByteArrayTranscoder();

	private Properties props;

	public static void main(String[] args) throws Exception {
		ValkyrieRebalance vr = new ValkyrieRebalance();
		CmdLineParser parser = new CmdLineParser(vr);
		parser.parseArgument(args);
		Map<String, Long> stats = vr.call();
		for (Map.Entry<String, Long> entry : stats.entrySet()) {
			System.out.println(String.format("%1$s\t%2$d", entry.getKey(),
					entry.getValue()));
		}
		System.out.println("Completed successfully. Exiting.");
		System.exit(0);
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

		long examined = 0, moved = 0, notMoved = 0, getFailures = 0, setFailures = 0, deleteFailures = 0;
		int writeReplicas = Integer.parseInt(props
				.getProperty("write.replicas"));
		KeyValueStoreIterator keyIterator = src.iterkeys();
		try {
			Iterator<String> iter = keyIterator.iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				++examined;

				if (examined <= skip)
					continue;

				List<Node> preferenceList = valkyrie.getPreferenceList(key,
						writeReplicas);
				boolean set = true;

				for (Node node : preferenceList) {
					if (node.getId() == nodeId) {
						set = false;
						break;
					}
				}
				if (set) {
					byte[] bytes = null;
					boolean successfulMove = false;
					
					try {
						bytes = (byte[]) src.get(key, byteTranscoder);
					} catch (Exception e) {
						e.printStackTrace();
						++getFailures;
					}
					try {
						if (bytes != null) {
							valkyrie.set(key, bytes, byteTranscoder);
							successfulMove = true;
							++moved;
						}
					} catch (Exception e) {
						e.printStackTrace();
						++setFailures;
					}
					try {
						if (successfulMove && delete)
							src.delete(key);
					} catch (Exception e) {
						e.printStackTrace();
						++deleteFailures;
					}
				} else
					++notMoved;

				if (examined % 1000 == 0) {
					System.out.println("Status");
					System.out.println("examined: " + examined);
					System.out.println("moved: " + moved);
					System.out.println("getFailures: " + getFailures);
					System.out.println("setFailures: " + setFailures);
					System.out.println("deleteFailures: " + deleteFailures);
				}
				if ((max > 0) && (examined >= max))
					break;

				if (sleep > 0)
					Thread.sleep(sleep);
			}
		} finally {
			keyIterator.close();
		}
		Map<String, Long> results = new HashMap<String, Long>();
		results.put("examined", examined);
		results.put("moved", moved);
		results.put("not-moved", notMoved);
		results.put("get-failures", getFailures);
		results.put("set-failures", setFailures);
		results.put("delete-failures", deleteFailures);
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
