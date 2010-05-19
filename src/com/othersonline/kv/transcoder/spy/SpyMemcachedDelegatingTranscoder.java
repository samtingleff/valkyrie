package com.othersonline.kv.transcoder.spy;

import java.io.IOException;

import com.othersonline.kv.transcoder.SerializingTranscoder;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class SpyMemcachedDelegatingTranscoder<T> implements Transcoder<T> {

	private static final int MAX_SIZE = Integer.MAX_VALUE;

	private com.othersonline.kv.transcoder.Transcoder delegate = new SerializingTranscoder();

	public SpyMemcachedDelegatingTranscoder() {
	}

	public SpyMemcachedDelegatingTranscoder(
			com.othersonline.kv.transcoder.Transcoder delegate) {
		this.delegate = delegate;
	}

	public T decode(CachedData data) {
		if (data == null)
			return null;
		byte[] bytes = data.getData();
		if (bytes == null)
			return null;
		try {
			return ((T) delegate.decode(bytes));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public CachedData encode(T object) {
		try {
			byte[] bytes = delegate.encode(object);
			return new CachedData(0, bytes, MAX_SIZE);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean asyncDecode(CachedData cd) {
		return false;
	}

	public int getMaxSize() {
		return MAX_SIZE;
	}

}
