package com.rubiconproject.oss.kv.backends;

public interface KeyValueStoreIterator extends Iterable<String> {
	public void close();
}
