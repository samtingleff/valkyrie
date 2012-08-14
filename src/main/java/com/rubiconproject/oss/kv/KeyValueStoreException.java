package com.rubiconproject.oss.kv;

public class KeyValueStoreException extends Exception {
	private static final long serialVersionUID = 7716151934657756348L;

	public KeyValueStoreException() {
	}

	public KeyValueStoreException(String message) {
		super(message);
	}

	public KeyValueStoreException(Throwable cause) {
		super(cause);
	}

	public KeyValueStoreException(String message, Throwable cause) {
		super(message, cause);
	}
}
