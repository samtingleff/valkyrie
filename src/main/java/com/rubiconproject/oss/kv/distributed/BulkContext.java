package com.rubiconproject.oss.kv.distributed;

import java.util.Map;

public interface BulkContext<V> extends Context<V>{
	public BulkOperationResult<V> getBulkResult();
	public String[] getKeys();
	public Map<String,V> getValues();
	public void setValues(Map<String,V> values);
}
