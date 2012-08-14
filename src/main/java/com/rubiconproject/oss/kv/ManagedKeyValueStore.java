package com.rubiconproject.oss.kv;

public interface ManagedKeyValueStore extends KeyValueStore {
	public String getMXBeanObjectName();

	public Object getMXBean();
}
