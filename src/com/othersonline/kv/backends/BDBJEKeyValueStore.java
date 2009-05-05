package com.othersonline.kv.backends;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BDBJEKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "bdbje";

	private Log log = LogFactory.getLog(getClass());

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private File dir;

	private int cacheSize = 10 * 1024 * 1024;

	private EnvironmentConfig envConfig;

	private DatabaseConfig config;

	private Environment env;

	private Database db;

	private StoredClassCatalog classCatalog;

	private EntryBinding<byte[]> dataBinding;

	public BDBJEKeyValueStore() {
	}

	public BDBJEKeyValueStore(File dir) {
		this.dir = dir;
	}

	public void setDirectory(String directory) {
		this.dir = new File(directory);
	}

	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		try {
			if (!dir.exists()) {
				dir.mkdirs();
			}
			envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setCacheSize(cacheSize);
			envConfig.setSharedCache(true);
			envConfig.setTransactional(true);
			env = new Environment(dir, envConfig);
			this.config = new DatabaseConfig();
			config.setAllowCreate(env.getConfig().getAllowCreate());
			config.setTransactional(env.getConfig().getTransactional());
			config.setReadOnly(false);
			this.db = env.openDatabase(null, "name", config);
			this.classCatalog = new StoredClassCatalog(db);
			this.dataBinding = new SerialBinding<byte[]>(classCatalog,
					byte[].class);
			super.start();
		} catch (DatabaseException e) {
			log.error("DatabaseException inside start()", e);
			throw new IOException(e);
		}
	}

	public void stop() {
		super.stop();
		try {
			db.close();
			this.db = null;
		} catch (DatabaseException e) {
			log.error("DatabaseException inside stop()", e);
		} finally {
		}
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		Transaction tx = null;
		try {
			tx = env.beginTransaction(null, null);

			DatabaseEntry keyEntry = getDatabaseKeyEntry(key);
			DatabaseEntry dataEntry = new DatabaseEntry();
			OperationStatus result = db.get(tx, keyEntry, dataEntry,
					LockMode.READ_COMMITTED);
			boolean retval = (result.equals(OperationStatus.SUCCESS));

			tx.commit();
			return retval;
		} catch (DatabaseException e) {
			rollback(tx);
			log.error("DatabaseException inside exists()", e);
			throw new KeyValueStoreException(e);
		}
	}

	public Object get(String key) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		assertReadable();
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Transaction tx = null;
		try {
			tx = env.beginTransaction(null, null);

			DatabaseEntry keyEntry = getDatabaseKeyEntry(key);
			DatabaseEntry dataEntry = new DatabaseEntry();
			OperationStatus result = db.get(tx, keyEntry, dataEntry,
					LockMode.READ_COMMITTED);
			byte[] data = null;
			if (result.equals(OperationStatus.NOTFOUND))
				data = null;
			else if (result.equals(OperationStatus.KEYEMPTY))
				data = null;
			else if (result.equals(OperationStatus.SUCCESS))
				data = (byte[]) dataBinding.entryToObject(dataEntry);

			tx.commit();
			if (data != null) {
				Object obj = transcoder.decode(data);
				return obj;
			} else {
				return null;
			}
		} catch (DatabaseException e) {
			rollback(tx);
			log.error("DatabaseException inside exists()", e);
			throw new KeyValueStoreException(e);
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		List<String> coll = Arrays.asList(keys);
		return getBulk(coll);
	}

	public Map<String, Object> getBulk(final List<String> keys)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		return getBulk(keys, defaultTranscoder);
	}

	public Map<String, Object> getBulk(final List<String> keys,
			Transcoder transcoder) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		Transaction tx = null;
		try {
			tx = env.beginTransaction(null, null);

			for (String key : keys) {
				DatabaseEntry keyEntry = getDatabaseKeyEntry(key);
				DatabaseEntry dataEntry = new DatabaseEntry();
				OperationStatus result = db.get(tx, keyEntry, dataEntry,
						LockMode.READ_COMMITTED);
				byte[] data = null;
				if (result.equals(OperationStatus.NOTFOUND))
					data = null;
				else if (result.equals(OperationStatus.KEYEMPTY))
					data = null;
				else if (result.equals(OperationStatus.SUCCESS))
					data = (byte[]) dataBinding.entryToObject(dataEntry);
				if (data != null) {
					Object obj = transcoder.decode(data);
					results.put(key, obj);
				}
			}
			tx.commit();
			return results;
		} catch (DatabaseException e) {
			rollback(tx);
			log.error("DatabaseException inside exists()", e);
			throw new KeyValueStoreException(e);
		}
	}

	public void set(String key, Object value)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		Transaction tx = null;
		try {
			byte[] bytes = transcoder.encode(value);
			tx = env.beginTransaction(null, null);
			DatabaseEntry keyEntry = getDatabaseKeyEntry(key);
			db.put(tx, keyEntry, getDatabaseEntry(bytes));
			tx.commit();
		} catch (DatabaseException e) {
			rollback(tx);
			log.error(String.format("DatabaseException inside put(%1$s)", key),
					e);
			throw new KeyValueStoreException(e);
		}
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		Transaction tx = null;
		try {
			tx = env.beginTransaction(null, null);
			DatabaseEntry keyEntry = getDatabaseKeyEntry(key);
			db.delete(tx, keyEntry);
			tx.commit();
		} catch (DatabaseException e) {
			rollback(tx);
			log.error(String.format("DatabaseException inside delete(%1$s)",
					key), e);
			throw new KeyValueStoreException(e);
		}
	}

	private DatabaseEntry getDatabaseKeyEntry(String key) {
		return new DatabaseEntry(key.getBytes());
	}

	private DatabaseEntry getDatabaseEntry(byte[] data) {
		DatabaseEntry entry = new DatabaseEntry();
		dataBinding.objectToEntry(data, entry);
		return entry;
	}

	private void rollback(Transaction tx) {
		if (tx != null) {
			try {
				tx.abort();
			} catch (Exception e) {
				log.warn("Exception calling abort()", e);
			}
		}
	}

}
