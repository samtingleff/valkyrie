package com.othersonline.kv.distributed;

import java.util.Map;

public interface BulkOperationResult<V>  extends OperationResult<V> {

	public BulkOperation<V> getBulkOperation();
	public Map<String,V> getValues();
}
