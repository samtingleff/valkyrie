package com.rubiconproject.oss.kv.util;

import java.util.concurrent.ThreadFactory;

/**
 * A ThreadFactory that simply creates new threads and calls setDaemon(true)
 * before returning.
 * 
 * @author sam
 * 
 */
public class DaemonThreadFactory implements ThreadFactory {

	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		t.setDaemon(true);
		return t;
	}

}
