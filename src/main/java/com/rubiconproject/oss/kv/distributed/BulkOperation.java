package com.rubiconproject.oss.kv.distributed;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

public interface BulkOperation<V> extends Operation<V> {
	public String[] getKeys();
}
