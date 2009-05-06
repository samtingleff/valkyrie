package com.othersonline.kv.distributed.test;

import com.othersonline.kv.distributed.HashAlgorithm;

public class HashCodeHashAlgorithm implements HashAlgorithm {

	public long hash(String key) {
		return (long) key.hashCode();
	}

}
