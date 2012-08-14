package com.othersonline.kv.tx;

import java.io.IOException;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.transcoder.Transcoder;

public interface TransactionalKeyValueStore extends KeyValueStore {
	public <T> KeyValueStoreTransaction<T> txGet(String key)
			throws KeyValueStoreException, KeyValueStoreTransactionException,
			IOException, ClassNotFoundException;;

	public <T> KeyValueStoreTransaction<T> txGet(String key,
			Transcoder transcoder) throws KeyValueStoreException,
			KeyValueStoreTransactionException, IOException,
			ClassNotFoundException;

	public <T> void txSet(KeyValueStoreTransaction<T> tx, String key)
			throws KeyValueStoreException, KeyValueStoreTransactionException,
			IOException, ClassNotFoundException;

	public <T> void txSet(KeyValueStoreTransaction<T> tx, String key,
			Transcoder transcoder) throws KeyValueStoreException,
			KeyValueStoreTransactionException, IOException,
			ClassNotFoundException;
}
