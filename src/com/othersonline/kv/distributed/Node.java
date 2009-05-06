package com.othersonline.kv.distributed;

import java.io.Serializable;

public interface Node extends Serializable {
	public int getId();

	public String getConnectionURI();
}
