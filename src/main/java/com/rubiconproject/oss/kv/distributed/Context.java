package com.rubiconproject.oss.kv.distributed;

public interface Context<V> {
	public OperationResult<V> getResult();

	public Node getSourceNode();

	public int getNodeRank();

	public int getVersion();

	public String getKey();

	public V getValue();

	public void setValue(V value);
}
