package com.othersonline.kv.distributed.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.distributed.Node;

public class OperationLog {
	private static OperationLog instance = new OperationLog();

	private static Log requestLog = LogFactory.getLog("haymitch.operationlog");

	private static Log preferenceLog = LogFactory
			.getLog("haymitch.preferencelog");

	public static OperationLog getInstance() {
		return instance;
	}

	public void log(String key, String op, long duration, boolean success) {
		if (requestLog.isInfoEnabled())
			requestLog.info(String.format("%1$s,%2$s,%3$d,%4$s", key, op,
					duration, success));
	}

	public void logPreferenceList(String key, List<Node> preferenceList) {
		if (preferenceLog.isInfoEnabled()) {
			// hack to avoid list iteration for common cases
			if (preferenceList.size() == 1)
				preferenceLog.info(String.format("%1$s,%2$d", key,
						preferenceList.get(0).getId()));
			else if (preferenceList.size() == 2)
				preferenceLog.info(String.format("%1$s,%2$d,%3$d", key,
						preferenceList.get(0).getId(), preferenceList.get(1)
								.getId()));
			else if (preferenceList.size() == 3)
				preferenceLog.info(String.format("%1$s,%2$s,%3$d,%4$d", key,
						preferenceList.get(0).getId(), preferenceList.get(1)
								.getId(), preferenceList.get(2).getId()));
			else if (preferenceList.size() > 3) {
				StringBuffer sb = new StringBuffer();
				sb.append(key);
				for (Node node : preferenceList) {
					sb.append(',');
					sb.append(node.getId());
				}
				preferenceLog.info(sb.toString());
			}
		}
	}
}
