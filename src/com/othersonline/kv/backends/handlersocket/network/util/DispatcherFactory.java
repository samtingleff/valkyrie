package com.othersonline.kv.backends.handlersocket.network.util;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;

import com.othersonline.kv.backends.handlersocket.network.core.impl.PoolDispatcher;

/**
 * Dispatcher Factory
 * 
 * @author dennis
 * 
 */
public class DispatcherFactory {
	public static com.othersonline.kv.backends.handlersocket.network.core.Dispatcher newDispatcher(
			int size, RejectedExecutionHandler rejectedExecutionHandler,String prefix) {
		if (size > 0) {
			return new PoolDispatcher(size, 60, TimeUnit.SECONDS,
					rejectedExecutionHandler,prefix);
		} else {
			return null;
		}
	}
}
