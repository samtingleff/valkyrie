package com.rubiconproject.oss.kv.tx;

import java.io.IOException;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreException;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

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
