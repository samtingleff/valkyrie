package com.othersonline.kv;

import java.io.IOException;

import com.othersonline.kv.transcoder.Transcoder;

public abstract class BaseKeyValueStore implements KeyValueStore {
	protected KeyValueStoreStatus status = KeyValueStoreStatus.Offline;

	public void start() throws IOException {
		this.status = KeyValueStoreStatus.Online;
	}

	public void stop() {
		this.status = KeyValueStoreStatus.Offline;
	}

	public KeyValueStoreStatus getStatus() {
		return status;
	}

	public void setStatus(KeyValueStoreStatus status) {
		this.status = status;
	}

	protected void assertWriteable() throws KeyValueStoreUnavailable {
		if (!status.equals(KeyValueStoreStatus.Online))
			throw new KeyValueStoreUnavailable();
	}

	protected void assertReadable() throws KeyValueStoreUnavailable {
		if (!(status.equals(KeyValueStoreStatus.Online) || (status
				.equals(KeyValueStoreStatus.ReadOnly))))
			throw new KeyValueStoreUnavailable();
	}

	public abstract String getIdentifier();

	public abstract boolean exists(String key) throws KeyValueStoreException,
			IOException;

	public abstract Object get(String key) throws KeyValueStoreException,
			IOException;

	public abstract Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException;

	public abstract void set(String key, Object value)
			throws KeyValueStoreException, IOException;

	public abstract void set(String key, Object value,
			Transcoder transcoder) throws KeyValueStoreException, IOException;

	public abstract void delete(String key) throws KeyValueStoreException,
			IOException;
}
