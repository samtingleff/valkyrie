package com.othersonline.kv.tx;

public interface KeyValueStoreTransaction<T> {
	public T getObject();

	public void setObject(T object);
}
