package com.rubiconproject.oss.kv.transcoder.spy;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class SpyMemcachedByteArrayTranscoder implements Transcoder<byte[]> {

	private static final int MAX_SIZE = Integer.MAX_VALUE;

	public byte[] decode(CachedData data) {
		return data.getData();
	}

	public CachedData encode(byte[] bytes) {
		CachedData cd = new CachedData(0, bytes, MAX_SIZE);
		return cd;
	}

	public boolean asyncDecode(CachedData cd) {
		return false;
	}

	public int getMaxSize() {
		return MAX_SIZE;
	}
}
