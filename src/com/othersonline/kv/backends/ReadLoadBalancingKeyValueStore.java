package com.othersonline.kv.backends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.transcoder.Transcoder;

public class ReadLoadBalancingKeyValueStore extends BaseManagedKeyValueStore
		implements KeyValueStore {
	public static final String IDENTIFIER = "readloadbalancing";

	protected KeyValueStore master;

	protected List<KeyValueStore> readers;

	protected Random random = new Random();

	public ReadLoadBalancingKeyValueStore() {
		this.readers = new ArrayList<KeyValueStore>();
	}

	public ReadLoadBalancingKeyValueStore(KeyValueStore master) {
		this.readers = new ArrayList<KeyValueStore>();
		this.master = master;
	}

	public ReadLoadBalancingKeyValueStore(KeyValueStore master,
			List<KeyValueStore> readers) {
		this.master = master;
		this.readers = readers;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void setMaster(KeyValueStore master) {
		this.master = master;
	}

	public void addReader(KeyValueStore reader) {
		readers.add(reader);
	}

	public void removeReader(KeyValueStore reader) {
		readers.remove(reader);
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		boolean exists = false;
		try {
			exists = getReader().exists(key);
		} catch (Exception e) {
			exists = master.exists(key);
		}
		return exists;
	}

	public Object get(String key) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		assertReadable();
		Object obj = null;
		try {
			obj = getReader().get(key);
		} catch (Exception e) {
			obj = master.get(key);
		}
		return obj;
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Object obj = null;
		try {
			obj = getReader().get(key, transcoder);
		} catch (Exception e) {
			obj = master.get(key, transcoder);
		}
		return obj;
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Map<String, Object> results = null;
		try {
			results = getReader().getBulk(keys);
		} catch (Exception e) {
			results = master.getBulk(keys);
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Map<String, Object> results = null;
		try {
			results = getReader().getBulk(keys);
		} catch (Exception e) {
			results = master.getBulk(keys);
		}
		return results;
	}

	public Map<String, Object> getBulk(List<String> keys, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Map<String, Object> results = null;
		try {
			results = getReader().getBulk(keys, transcoder);
		} catch (Exception e) {
			results = master.getBulk(keys, transcoder);
		}
		return results;
	}

	public void set(String key, Object value)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		master.set(key, value);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		master.set(key, value, transcoder);
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		master.delete(key);
	}

	protected KeyValueStore getReader() {
		if (readers == null)
			return master;
		int size = readers.size();
		if (size == 0)
			return master;
		else {
			int index = random.nextInt(size);
			return readers.get(index);
		}
	}

}
