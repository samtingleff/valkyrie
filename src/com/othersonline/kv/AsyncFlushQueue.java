package com.othersonline.kv;

import com.othersonline.kv.transcoder.Transcoder;

/**
 * An AsyncFlushQueue is responsible for performing asynchronous writes to a
 * KeyValueStore.
 * 
 * @author sam
 * 
 */
public interface AsyncFlushQueue {

	public void set(String key, Object value);

	public void set(String key, Object value, Transcoder transcoder);

	public void delete(String key);
}
