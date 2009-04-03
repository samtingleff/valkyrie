package com.othersonline.kv;

public interface ManagedKeyValueStore extends KeyValueStore {
	public String getMXBeanObjectName();

	public Object getMXBean();
}
