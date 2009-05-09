package com.othersonline.kv.distributed;

import java.io.Serializable;
import java.util.List;

public interface Node extends Serializable {
	public int getId();

	public int getPhysicalId();

	//public String getIdentifier();

	public String getConnectionURI();

	public List<Integer> getLogicalPartitionList();
}
