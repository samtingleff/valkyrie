package com.othersonline.kv.transcoder.xmemcached;

import net.rubyeye.xmemcached.transcoders.CachedData;
import net.rubyeye.xmemcached.transcoders.Transcoder;

public class XMemcachedByteArrayTranscoder implements Transcoder<byte[]> {

	public byte[] decode(CachedData data) {
		return data.getData();
	}

	public CachedData encode(byte[] bytes) {
		CachedData cd = new CachedData(0, bytes);
		return cd;
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
