package com.othersonline.kv.transcoder.spy;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class SpyMemcachedByteArrayTranscoder implements Transcoder<byte[]> {

	public byte[] decode(CachedData data) {
		return data.getData();
	}

	public CachedData encode(byte[] bytes) {
		CachedData cd = new CachedData(0, bytes);
		return cd;
	}
}
