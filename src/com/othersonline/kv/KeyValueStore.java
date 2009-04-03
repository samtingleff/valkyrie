package com.othersonline.kv;

import java.io.IOException;

import com.othersonline.kv.transcoder.Transcoder;

/**
 * A simple key->value json blob store.
 * 
 * @author sam
 * 
 */
public interface KeyValueStore {
	/**
	 * Return a unique string identifying this storage backend.
	 * 
	 * @return name of this storage backend
	 */
	public String getIdentifier();

	/**
	 * Perform any necessary initialization.
	 * 
	 * @throws IOException
	 */
	public void start() throws IOException;

	/**
	 * Shutdown.
	 * 
	 */
	public void stop();

	/**
	 * Get status of this key-value store.
	 * 
	 * @return current status of this backend
	 */
	public KeyValueStoreStatus getStatus();

	/**
	 * Set status of this key-value store.
	 * 
	 * @param status
	 */
	public void setStatus(KeyValueStoreStatus status);

	/**
	 * Determine whether or not a given key exists. On backends that do not
	 * provide native support for this primitive (memcached, tokyo tyrant) we
	 * will call get() and compare to null. This may be a more expensive
	 * operation than you would like.
	 * 
	 * @throws KeyValueStoreException
	 * @throws IOException
	 */
	public boolean exists(String key) throws KeyValueStoreException,
			IOException;

	/**
	 * Retrieve an object from store for a given key. Returns null if none
	 * found.
	 * 
	 * @throws KeyValueStoreException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * 
	 */
	public Object get(String key) throws KeyValueStoreException, IOException,
			ClassNotFoundException;

	/**
	 * Retrieve an object from store for a given key using the specified
	 * transcoder. Returns null if none found.
	 * 
	 * @throws KeyValueStoreException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * 
	 */
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException;

	/**
	 * Save an object for a given key.
	 * 
	 * @throws KeyValueStoreException
	 * @throws IOException
	 */
	public void set(String key, Object value) throws KeyValueStoreException,
			IOException;

	/**
	 * Save an object for a given key using the specified transcoder.
	 * 
	 * @throws KeyValueStoreException
	 * @throws IOException
	 */
	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException;

	/**
	 * Delete an object for a given key.
	 * 
	 * @param key
	 * @throws KeyValueStoreException
	 * @throws IOException
	 */
	public void delete(String key) throws KeyValueStoreException, IOException;
}
