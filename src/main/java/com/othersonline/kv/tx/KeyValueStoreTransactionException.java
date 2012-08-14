package com.othersonline.kv.tx;

import com.othersonline.kv.KeyValueStoreException;

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
