package com.othersonline.kv;

import java.io.Serializable;

import com.othersonline.kv.transcoder.Transcoder;

/**
 * An AsyncFlushQueue is responsible for performing asynchronous writes to a
 * KeyValueStore.
 * 
 * @author sam
 * 
 */
public interface AsyncFlushQueue {

	public void set(String key, Serializable value);

	public void set(String key, Serializable value, Transcoder transcoder);

	public void delete(String key);
}
