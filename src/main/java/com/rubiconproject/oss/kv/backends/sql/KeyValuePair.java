package com.rubiconproject.oss.kv.backends.sql;

public class KeyValuePair {

	private final String key;

	private final Object value;

	public KeyValuePair(final String key, final Object value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public Object getValue() {
		return value;
	}
}
