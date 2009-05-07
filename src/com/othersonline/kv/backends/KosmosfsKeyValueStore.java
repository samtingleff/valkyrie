package com.othersonline.kv.backends;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.kosmix.kosmosfs.access.KfsAccess;
import org.kosmix.kosmosfs.access.KfsInputChannel;
import org.kosmix.kosmosfs.access.KfsOutputChannel;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class KosmosfsKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "kosmosfs";

	private static Random random = new Random();

	private KfsAccess kfs;

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private String metaServerHost = "localhost";

	private int metaServerPort = 20000;

	private int gcFrequency = 5;

	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Configurable(name = "metaServerHost", accepts = Type.StringType)
	public void setMetaServerHost(String host) {
		this.metaServerHost = host;
	}

	@Configurable(name = "metaServerPort", accepts = Type.IntType)
	public void setMetaServerPort(int port) {
		this.metaServerPort = port;
	}

	@Configurable(name = "gcFrequency", accepts = Type.IntType)
	public void setGcFrequency(int gcFrequency) {
		this.gcFrequency = gcFrequency;
	}

	@Override
	public void start() throws IOException {
		kfs = new KfsAccess(metaServerHost, metaServerPort);
		super.start();
	}

	@Override
	public void stop() {
		kfs.release();
		kfs = null;
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		return kfs.kfs_exists(key);
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		KfsInputChannel ch = kfs.kfs_open(key);
		if (ch == null)
			return null;
		try {
			long size = kfs.kfs_filesize(key);
			ByteBuffer buffer = ByteBuffer.allocate((int) size);
			int read = ch.read(buffer);
			byte[] bytes = new byte[read];
			buffer.position(0);
			buffer.get(bytes);
			Object obj = transcoder.decode(bytes);
			return obj;
		} finally {
			try {
				ch.close();
			} catch (Exception e) {
			}
			gc();
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key, transcoder);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		byte[] bytes = transcoder.encode(value);
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		KfsOutputChannel ch = null;
		try {
			ch = kfs.kfs_create(key);
			if (ch == null) {
				mkdirs(key);
				ch = kfs.kfs_create(key);
			}
			int written = ch.write(buffer);
		} finally {
			try {
				ch.close();
			} catch (Exception e) {
			}
			gc();
		}
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		int result = kfs.kfs_remove(key);
	}

	private void mkdirs(String key) {
		int lastIndex = key.lastIndexOf('/');
		if (lastIndex > 0) {
			String parent = key.substring(0, lastIndex);
			kfs.kfs_mkdirs(parent);
		}
	}

	/**
	 * The KFS JNI wrapper likes to create lots of DirectByteBuffers. This will
	 * cause OutOfMemory exceptions unless you call System.gc() once in a while.
	 * Use the gcFrequency setting to control how often.
	 */
	private void gc() {
		if (random.nextInt(100) < gcFrequency) {
			System.gc();
		}
	}
}
