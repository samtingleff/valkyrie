package com.othersonline.kv.mgmt;

import java.io.IOException;

public interface KeyValueStoreMXBean {

	public void start() throws IOException;

	public void stop();

	public String getStatus();

	public void offline();

	public void readOnly();

	public void online();
}
