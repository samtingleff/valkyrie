package com.othersonline.kv;

public class KeyValueStoreUnavailable extends KeyValueStoreException {
	private static final long serialVersionUID = 4995482173973162850L;

	public KeyValueStoreUnavailable() {
		super();
	}

	public KeyValueStoreUnavailable(String message) {
		super(message);
	}

	public KeyValueStoreUnavailable(Throwable cause) {
		super(cause);
	}
}
