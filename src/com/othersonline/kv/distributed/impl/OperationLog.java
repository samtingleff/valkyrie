package com.othersonline.kv.distributed.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OperationLog {
	private static OperationLog instance = new OperationLog();

	private static Log log = LogFactory.getLog("hermione.operationlog");

	public static OperationLog getInstance() {
		return instance;
	}

	public void log(String key, int nodeid, String op, boolean success,
			long duration) {
		log.info(String.format("%1$s,%2$d,%3$s,%4$s,%5$d", key, nodeid, op,
				success, duration));
	}
}
