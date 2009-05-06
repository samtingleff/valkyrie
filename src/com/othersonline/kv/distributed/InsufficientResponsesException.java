package com.othersonline.kv.distributed;

public class InsufficientResponsesException extends
		DistrubutedKeyValueStoreException {
	private static final long serialVersionUID = -3845940900100650405L;

	private int required;

	private int received;

	public InsufficientResponsesException(int required, int received) {
		super();
		this.required = required;
		this.received = received;
	}

	public int getRequired() {
		return required;
	}

	public int getReceived() {
		return received;
	}
}
