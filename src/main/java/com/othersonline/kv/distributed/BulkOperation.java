package com.othersonline.kv.distributed;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.transcoder.Transcoder;

public interface BulkOperation<V> extends Operation<V> {
	public String[] getKeys();
}
