package com.rubiconproject.oss.kv.tx;

import com.rubiconproject.oss.kv.KeyValueStoreException;

public class KeyValueStoreTransactionException extends KeyValueStoreException {
	private static final long serialVersionUID = -3099781135216900480L;

	public KeyValueStoreTransactionException() {
	}

	public KeyValueStoreTransactionException(String message) {
		super(message);
	}

	public KeyValueStoreTransactionException(Throwable cause) {
		super(cause);
	}
}
