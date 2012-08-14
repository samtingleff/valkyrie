package com.othersonline.kv.distributed;

import com.othersonline.kv.KeyValueStoreException;

public class DistributedKeyValueStoreException extends KeyValueStoreException {
	private static final long serialVersionUID = -3640406019375304551L;

	public DistributedKeyValueStoreException() {
	}

	public DistributedKeyValueStoreException(String msg) {
		super(msg);
	}

	public DistributedKeyValueStoreException(Throwable root) {
		super(root);
	}
}
