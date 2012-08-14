package com.rubiconproject.oss.kv.distributed.hashing;

public interface HashAlgorithm {

	public long hash(final String key);
}
