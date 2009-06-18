package com.othersonline.kv.distributed.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OperationLog {
	private static OperationLog instance = new OperationLog();

	private static Log log = LogFactory.getLog("haymitch.operationlog");

	public static OperationLog getInstance() {
		return instance;
	}

	public void log(String key, String op, long duration,
			boolean success) {
		log.info(String.format("%1$s,%2$s,%3$d,%4$s", key, op,
				duration, success));
	}
}
