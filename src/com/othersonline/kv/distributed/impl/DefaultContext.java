package com.othersonline.kv.distributed.impl;

import com.othersonline.kv.distributed.Context;

public class DefaultContext<V> implements Context<V> {
	private int version;

	private V value;

	public DefaultContext(int version, V value) {
		this.version = version;
		this.value = value;
	}

	public int getVersion() {
		return version;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

}
