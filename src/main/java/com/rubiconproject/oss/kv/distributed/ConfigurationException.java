package com.rubiconproject.oss.kv.distributed;

public class ConfigurationException extends DistributedKeyValueStoreException {
	private static final long serialVersionUID = -1030730278244878326L;

	public ConfigurationException() {
	}

	public ConfigurationException(String msg) {
		super(msg);
	}

	public ConfigurationException(Throwable root) {
		super(root);
	}
}
