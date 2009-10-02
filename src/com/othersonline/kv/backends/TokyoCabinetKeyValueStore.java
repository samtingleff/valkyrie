package com.othersonline.kv.backends;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tokyocabinet.BDB;
import tokyocabinet.DBM;
import tokyocabinet.HDB;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class TokyoCabinetKeyValueStore extends BaseManagedKeyValueStore implements IterableKeyValueStore {
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
		if ((btree) || (path.endsWith(".tcb"))) {
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

	public String getFile() {
		return path;
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

	public KeyValueStoreIterator iterkeys() throws KeyValueStoreUnavailable {
		assertReadable();
		if (!dbm.iterinit())
			return new NullIterator();
		else {
			return new TokyoCabinetIterator(dbm);
		}
	}

	public List<String> fwmkeys(String prefix, int max)
			throws KeyValueStoreException {
		assertReadable();
		return dbm.fwmkeys(prefix, max);
	}

	public int addint(String key, int num) throws KeyValueStoreUnavailable {
		assertReadable();
		return dbm.addint(key, num);
	}

	public double adddouble(String key, double num)
			throws KeyValueStoreUnavailable {
		assertReadable();
		return dbm.adddouble(key, num);
	}

	public long rnum() throws KeyValueStoreUnavailable {
		assertReadable();
		return dbm.rnum();
	}

	public long fsiz() throws KeyValueStoreUnavailable {
		assertReadable();
		return dbm.fsiz();
	}

	public boolean sync() throws KeyValueStoreUnavailable {
		assertWriteable();
		if (btree)
			return bdb.sync();
		else
			return hdb.sync();
	}

	public boolean optimize() throws KeyValueStoreUnavailable {
		assertWriteable();
		if (btree)
			return bdb.optimize();
		else
			return hdb.optimize();
	}

	public boolean vanish() throws KeyValueStoreUnavailable {
		assertWriteable();
		if (btree)
			return bdb.vanish();
		else
			return hdb.vanish();
	}

	private class TokyoCabinetIterator implements KeyValueStoreIterator, Iterator<String> {
		private DBM dbm;
		private String next;
		public TokyoCabinetIterator(DBM dbm) {
			this.dbm = dbm;
		}

		public Iterator<String> iterator() {
			return this;
		}
		public void close() {
		}

		public boolean hasNext() {
			next = dbm.iternext2();
			return (next != null);
		}

		public String next() {
			return next;
		}

		public void remove() {
			dbm.out(next);
		}
		
	}
	private static class NullIterator implements KeyValueStoreIterator, Iterator<String> {
		public Iterator<String> iterator() {
			return this;
		}

		public boolean hasNext() {
			return false;
		}

		public String next() {
			return null;
		}

		public void remove() {
		}

		public void close() {
		}
	}
}
