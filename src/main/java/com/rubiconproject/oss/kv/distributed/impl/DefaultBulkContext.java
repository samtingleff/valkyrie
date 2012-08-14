package com.rubiconproject.oss.kv.distributed.impl;

import java.util.Map;

import com.rubiconproject.oss.kv.distributed.BulkContext;
import com.rubiconproject.oss.kv.distributed.BulkOperationResult;
import com.rubiconproject.oss.kv.distributed.Node;

public class DefaultBulkContext<V> extends DefaultContext<V> implements BulkContext<V> {

	private String[] keys;
	private Map<String,V> values;

	public DefaultBulkContext(BulkOperationResult<V> result, Node source, int nodeRank,
			int version, String[] keys, Map<String,V> values) {
		super(result,source,nodeRank,version,null,null);
		this.keys = keys;
		this.values = values;
	}

	public String[] getKeys()
	{
		return keys;
	}

	public void setKeys(String[] keys)
	{
		this.keys = keys;
	}

	public Map<String, V> getValues()
	{
		return values;
	}

	public void setValues(Map<String, V> values)
	{
		this.values = values;
	}

	public BulkOperationResult<V> getBulkResult()
	{
		return (BulkOperationResult<V>)getResult();
	}

}
