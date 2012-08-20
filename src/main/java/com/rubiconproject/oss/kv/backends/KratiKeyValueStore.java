package com.rubiconproject.oss.kv.backends;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rubiconproject.oss.kv.BaseManagedKeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.KeyValueStoreUnavailable;
import com.rubiconproject.oss.kv.annotations.Configurable;
import com.rubiconproject.oss.kv.annotations.Configurable.Type;
import com.rubiconproject.oss.kv.transcoder.SerializableTranscoder;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

import krati.core.segment.ChannelSegmentFactory;
import krati.store.DataStore;
import krati.store.DynamicDataStore;
import krati.util.FnvHashFunction;

public class KratiKeyValueStore extends BaseManagedKeyValueStore
		implements KeyValueStore, Iterable<Map.Entry<byte[], byte[]>> {

	public static final String IDENTIFIER = "krati";

	private static Log log = LogFactory.getLog(KratiKeyValueStore.class);

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private DataStore<byte[], byte[]> store;

	private String dir;

	private int initLevel = 5;

	private int batchSize = 100;

	private int numSyncBatches = 5;

	private int segmentFileSizeMB = 64;

	private double segmentCompactFactor = 0.5;

	private double hashLoadFactor = 0.75;

	public KratiKeyValueStore(String dir,
			int initLevel,
			int batchSize,
			int numSyncBatches,
			int segmentFileSizeMB,
			double segmentCompactFactor,
			double hashLoadFactor) throws Exception {
		this.dir = dir;
		this.initLevel = initLevel;
		this.batchSize = batchSize;
		this.numSyncBatches = numSyncBatches;
		this.segmentFileSizeMB = segmentFileSizeMB;
		this.segmentCompactFactor = segmentCompactFactor;
		this.hashLoadFactor = hashLoadFactor;
	}

	public KratiKeyValueStore() {
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void start() throws IOException {
		try {
			store = createDataStore(new File(dir),
					initLevel,
					batchSize,
					numSyncBatches,
					segmentFileSizeMB,
					segmentCompactFactor,
					hashLoadFactor);
			super.start();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void stop() {
		try {
			store.close();
			super.stop();
		} catch (IOException e) {
			log.warn("IOException calling close()", e);
		}
	}

	@Override
	public Object get(String key) throws IOException, KeyValueStoreUnavailable {
		assertReadable();
		return get(key, defaultTranscoder);
	}

	@Override
	public Object get(String key, Transcoder transcoder) throws IOException, KeyValueStoreUnavailable {
		assertReadable();
		byte[] bytes = store.get(key.getBytes());
		if (bytes == null)
			return null;
		else {
			Object obj = transcoder.decode(bytes);
			return obj;
		}
	}

	@Override
	public void delete(String key) throws IOException, KeyValueStoreUnavailable {
		assertWriteable();
		try {
			store.delete(key.getBytes());
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		return getBulk(Arrays.asList(keys));
	}

	@Override
	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		return getBulk(keys, defaultTranscoder);
	}

	@Override
	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> result = new HashMap<String, Object>(keys.size());
		for (String k : keys) {
			Object obj = get(k, transcoder);
			if (obj != null)
				result.put(k, obj);
		}
		return result;
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		byte[] bytes = store.get(key.getBytes());
		return (bytes == null) ? false : true;
	}

	@Override
	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		set(key, value, defaultTranscoder);
	}

	@Override
	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		byte[] bytes = null;
		if (value instanceof byte[])
			bytes = (byte[]) value;
		else
			bytes = transcoder.encode(value);
		try {
			store.put(key.getBytes(), bytes);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public void sync() throws IOException {
		store.sync();
	}

	public void persist() throws IOException {
		store.persist();
	}

	public void clear() throws IOException {
		store.clear();
	}

	public Iterator<Map.Entry<byte[], byte[]>> iterator() {
		return store.iterator();
	}

	protected DataStore<byte[], byte[]> createDataStore(File dir,
			int initLevel,
			int batchSize,
			int numSyncBatches,
			int segmentFileSizeMB,
			double segmentCompactFactor,
			double hashLoadFactor) throws Exception {
		// reasonable dev values:
		// initLevel: 0
		// batchSize: 100
		// numSyncBatches: 5
		// segmentFileSizeMB: 256
		// segmentCompactFactor: .5
		// hashLoadFactor: .75
		// reasonable production values:
		// initLevel: 10
		// batchSize: 10000
		// numSyncBatches: 5
		// segmentFileSizeMB: 256
		// segmentCompactFactor: .5
		// hashLoadFactor: .75
		
		// http://groups.google.com/group/krati/browse_thread/thread/fbc445367da4430f?pli=1
		return new DynamicDataStore(dir,
				initLevel,
				batchSize,
				numSyncBatches,
				segmentFileSizeMB,
				new ChannelSegmentFactory(),
				segmentCompactFactor,
				hashLoadFactor,
				new FnvHashFunction());
	}

	@Configurable(name = "dir", accepts = Type.StringType)
	public void setDir(String dir) {
		this.dir = dir;
	}

	@Configurable(name = "initLevel", accepts = Type.IntType)
	public void setInitLevel(int initLevel) {
		this.initLevel = initLevel;
	}

	@Configurable(name = "batchSize", accepts = Type.IntType)
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	@Configurable(name = "numSyncBatches", accepts = Type.IntType)
	public void setNumSyncBatches(int numSyncBatches) {
		this.numSyncBatches = numSyncBatches;
	}

	@Configurable(name = "segmentFileSizeMB", accepts = Type.IntType)
	public void setSegmentFileSizeMB(int segmentFileSizeMB) {
		this.segmentFileSizeMB = segmentFileSizeMB;
	}

	@Configurable(name = "segmentCompactFactor", accepts = Type.DoubleType)
	public void setSegmentCompactFactor(double segmentCompactFactor) {
		this.segmentCompactFactor = segmentCompactFactor;
	}

	@Configurable(name = "hashLoadFactor", accepts = Type.DoubleType)
	public void setHashLoadFactor(double hashLoadFactor) {
		this.hashLoadFactor = hashLoadFactor;
	}
}
