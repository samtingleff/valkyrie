package com.othersonline.kv.transcoder.spy;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class SpyMemcachedByteArrayTranscoder implements Transcoder<byte[]>
{

	@Override
	public byte[] decode(CachedData data)
	{
		return data.getData();
	}

	@Override
	public CachedData encode(byte[] bytes)
	{
		CachedData cd = new CachedData(0, bytes);
		return cd;
	}
}
