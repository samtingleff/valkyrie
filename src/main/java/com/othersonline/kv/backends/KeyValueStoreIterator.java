package com.othersonline.kv.backends;

public interface KeyValueStoreIterator extends Iterable<String> {
	public void close();
}
