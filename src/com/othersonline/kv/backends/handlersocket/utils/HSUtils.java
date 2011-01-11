package com.othersonline.kv.backends.handlersocket.utils;

public class HSUtils {
	private HSUtils() {

	}

	/**
	 * Whether string is a space or null
	 * 
	 * @param s
	 * @return
	 */
	public static final boolean isBlank(String s) {
		if (s == null || s.trim().length() == 0) {
			return true;
		}
		return false;
	}

}
