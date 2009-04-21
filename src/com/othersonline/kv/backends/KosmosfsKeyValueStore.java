package com.othersonline.kv.backends;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kosmix.kosmosfs.access.KfsAccess;
import org.kosmix.kosmosfs.access.KfsInputChannel;
import org.kosmix.kosmosfs.access.KfsOutputChannel;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class KosmosfsKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "kosmosfs";

	private KfsAccess kfs;

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private String metaServerHost = "localhost";

	private int metaServerPort = 20000;

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void setMetaServerHost(String host) {
		this.metaServerHost = host;
	}

	public void setMetaServerPort(int port) {
		this.metaServerPort = port;
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

	public Object get(String key) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		KfsInputChannel ch = kfs.kfs_open(key);
		if (ch == null)
			return null;
		try {
			long size = kfs.kfs_filesize(key);
			ByteBuffer buffer = ByteBuffer.allocate((int) size);
			int read = ch.read(buffer);
			byte[] bytes = buffer.array();
			Object obj = transcoder.decode(bytes);
			return obj;
		} finally {
			try {
				ch.close();
			} catch (Exception e) {
			}
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
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
			throws KeyValueStoreException, IOException, ClassNotFoundException {
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
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key, transcoder);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public void set(String key, Serializable value)
			throws KeyValueStoreException, IOException {
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Serializable value, Transcoder transcoder)
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
}
