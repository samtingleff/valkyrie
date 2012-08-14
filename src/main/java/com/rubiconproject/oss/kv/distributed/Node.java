package com.rubiconproject.oss.kv.distributed;

import java.io.Serializable;

public interface Node extends Comparable<Node>, Serializable {
	public int getId();

	public int getPhysicalId();

	public String getSalt();

	public String getConnectionURI();
}
