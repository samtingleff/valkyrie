package com.othersonline.kv.distributed;

public interface Context<V> {
	public Node getSourceNode();

	public int getNodeRank();

	public int getVersion();

	public String getKey();

	public V getValue();

	public void setValue(V value);
}
