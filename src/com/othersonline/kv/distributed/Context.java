package com.othersonline.kv.distributed;

public interface Context<V> {
	public int getVersion();

	public V getValue();

	public void setValue(V value);
}
