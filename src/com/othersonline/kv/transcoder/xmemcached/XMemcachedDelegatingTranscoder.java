package com.othersonline.kv.transcoder.xmemcached;

import java.io.IOException;

import net.rubyeye.xmemcached.transcoders.CachedData;
import net.rubyeye.xmemcached.transcoders.Transcoder;

import com.othersonline.kv.transcoder.SerializingTranscoder;

public class XMemcachedDelegatingTranscoder<T> implements Transcoder<T> {

	private com.othersonline.kv.transcoder.Transcoder delegate = new SerializingTranscoder();

	public T decode(CachedData data) {
		if (data == null)
			return null;
		byte[] bytes = data.getData();
		try {
			return ((T) delegate.decode(bytes));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public CachedData encode(T object) {
		try {
			byte[] bytes = delegate.encode(object);
			return new CachedData(0, bytes);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isPackZeros() {
		return false;
	}

	public boolean isPrimitiveAsString() {
		return false;
	}

	public void setCompressionThreshold(int arg0) {
	}

	public void setPackZeros(boolean arg0) {
	}

	public void setPrimitiveAsString(boolean arg0) {
	}
}
