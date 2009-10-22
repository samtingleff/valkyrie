package com.othersonline.kv.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.backends.IterableKeyValueStore;
import com.othersonline.kv.backends.KeyValueStoreIterator;
import com.othersonline.kv.backends.UriConnectionFactory;
import com.othersonline.kv.transcoder.ByteArrayTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

/**
 * Copy from one kv store to another.
 * 
 * Given an input node (by uri) and a destination node (by uri), will copy from
 * input to output.
 * 
 * Usage:
 * 
 * - Run it: java -classpath oo-kv-storage.jar:...
 * com.othersonline.kv.tools.NodeCopy --source "tyrant://dev-db:1978" --dest
 * "tyrant://dev-db:1979" --sleep 2
 * 
 * @author stingleff
 * 
 */
public class NodeCopy implements Runnable, Callable<Map<String, Long>> {

	@Option(name = "--source", usage = "Source uri (default: none)", required = true)
	private String source;

	@Option(name = "--dest", usage = "Destination uri (default: none)", required = true)
	private String dest;

	@Option(name = "--sleep", usage = "Sleep time between keys (millis) (default: disabled)")
	private long sleep = 0;

	@Option(name = "--skip", usage = "Skip this many records first (default: 0)")
	private int skip = 0;

	@Option(name = "--max", usage = "Stop after x keys (default: disabled)")
	private int max = -1;

	@Option(name = "--delete", usage = "Delete from source (default: false")
	private boolean delete = false;

	private Transcoder byteTranscoder = new ByteArrayTranscoder();

	public static void main(String[] args) throws Exception {
		NodeCopy vr = new NodeCopy();
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
		IterableKeyValueStore src = (IterableKeyValueStore) getKeyValueStore(source);
		KeyValueStore kv = getKeyValueStore(dest);

		long examined = 0, moved = 0, notMoved = 0, getFailures = 0, setFailures = 0, deleteFailures = 0;
		KeyValueStoreIterator keyIterator = src.iterkeys();
		try {
			Iterator<String> iter = keyIterator.iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				++examined;

				if (examined <= skip) {
					++notMoved;
					continue;
				}

				byte[] bytes = null;
				try {
					bytes = (byte[]) src.get(key, byteTranscoder);
				} catch (Exception e) {
					e.printStackTrace();
					++getFailures;
				}
				boolean successfulMove = false;
				try {
					if (bytes != null) {
						kv.set(key, bytes, byteTranscoder);
						successfulMove = true;
						++moved;
					} else
						++notMoved;
				} catch (Exception e) {
					e.printStackTrace();
					++setFailures;
				}

				try {
					if (successfulMove && delete) {
						src.delete(key);
					}
				} catch (Exception e) {
					e.printStackTrace();
					++deleteFailures;
				}

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
			src.stop();
			kv.stop();
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

	private KeyValueStore getKeyValueStore(String uri)
			throws KeyValueStoreUnavailable, IOException {
		UriConnectionFactory factory = new UriConnectionFactory();
		KeyValueStore kv = factory.getStore(null, uri);
		return kv;
	}
}
