package com.othersonline.kv.backends;

import java.io.IOException;
import java.net.InetSocketAddress;

import tokyotyrant.RDB;
import tokyotyrant.transcoder.SerializableTranscoder;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.transcoder.Transcoder;

public class TokyoTyrantKeyValueStore extends BaseManagedKeyValueStore implements
		KeyValueStore {
	public static final String IDENTIFIER = "tyrant";

	private RDB rdb;

	private tokyotyrant.transcoder.Transcoder transcoder = new SerializableTranscoder();

	private String host = "localhost";

	private int port = 1978;

	public TokyoTyrantKeyValueStore() {
	}

	public TokyoTyrantKeyValueStore(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void start() throws IOException {
		rdb = new RDB();
		rdb.open(new InetSocketAddress(host, port));
		super.start();
	}

	@Override
	public void stop() {
		rdb.close();
		rdb = null;
		super.stop();
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		return (rdb.get(key) != null);
	}

	@Override
	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		return rdb.get(key, transcoder);
	}

	@Override
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		return rdb.get(key, this.transcoder);
	}

	@Override
	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		rdb.put(key, value, transcoder);
	}

	@Override
	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		rdb.put(key, value, this.transcoder);
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		rdb.out(key);
	}

}
