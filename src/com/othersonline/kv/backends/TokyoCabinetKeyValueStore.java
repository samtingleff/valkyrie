package com.othersonline.kv.backends;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tokyocabinet.BDB;
import tokyocabinet.DBM;
import tokyocabinet.HDB;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class TokyoCabinetKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "tokyocabinet";

	private Log log = LogFactory.getLog(getClass());

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private String path;

	private boolean btree = false;

	private DBM dbm;

	private HDB hdb;

	private BDB bdb;

	public TokyoCabinetKeyValueStore() {
	}

	public TokyoCabinetKeyValueStore(String path) {
		this.path = path;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		if (btree) {
			bdb = new BDB();
			if (!bdb.open(path, BDB.OWRITER | BDB.OCREAT)) {
				int ecode = bdb.ecode();
				throw new IOException(String.format(
						"Could not open b-tree db. Error code: %1$d", ecode));
			}
			this.dbm = bdb;
		} else {
			hdb = new HDB();
			if (!hdb.open(path, HDB.OWRITER | HDB.OCREAT)) {
				int ecode = hdb.ecode();
				throw new IOException(String.format(
						"Could not open hash db. Error code: %1$d", ecode));
			}
			this.dbm = hdb;
		}
		super.start();
	}

	public void stop() {
		super.stop();
		if (btree)
			bdb.close();
		else
			hdb.close();
	}

	@Configurable(name = "file", accepts = Type.StringType)
	public void setFile(String path) {
		this.path = path;
	}

	@Configurable(name = "btree", accepts = Type.BooleanType)
	public void setBtree(boolean useBtree) {
		this.btree = useBtree;
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		return (dbm.get(key) != null);
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		byte[] bytes = dbm.get(key.getBytes());
		Object value = (bytes == null) ? null : transcoder.decode(bytes);
		return value;
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object value = get(key);
			if (value != null)
				results.put(key, value);
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return getBulk(keys, defaultTranscoder);
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object value = get(key, transcoder);
			if (value != null)
				results.put(key, value);
		}
		return results;
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		byte[] bytes = transcoder.encode(value);
		dbm.put(key.getBytes(), bytes);
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		dbm.out(key);
	}

}
