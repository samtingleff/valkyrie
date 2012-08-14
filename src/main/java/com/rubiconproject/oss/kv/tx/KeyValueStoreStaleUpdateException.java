package com.rubiconproject.oss.kv.tx;

public class KeyValueStoreStaleUpdateException extends
		KeyValueStoreTransactionException {
	private static final long serialVersionUID = -4059943684227358932L;

	public KeyValueStoreStaleUpdateException() {
	}

	public KeyValueStoreStaleUpdateException(String message) {
		super(message);
	}

	public KeyValueStoreStaleUpdateException(Throwable cause) {
		super(cause);
	}
}
