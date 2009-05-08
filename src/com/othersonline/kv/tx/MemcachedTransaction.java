package com.othersonline.kv.tx;

import net.spy.memcached.CASValue;

public class MemcachedTransaction<T> implements KeyValueStoreTransaction<T> {

	private CASValue<T> cas;

	private T object;
	public MemcachedTransaction(CASValue<T> cas) {
		this.cas = cas;
	}
	public long getCasId() {
		return (cas == null) ? 0 : cas.getCas();
	}
	public T getObject() {
		return (cas == null) ? null : cas.getValue();
	}

	public void setObject(T object) {
		this.object = object;
	}
}
