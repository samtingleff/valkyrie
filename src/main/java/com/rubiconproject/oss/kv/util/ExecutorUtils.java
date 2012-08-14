package com.rubiconproject.oss.kv.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExecutorUtils {
	private static Log log = LogFactory.getLog(ExecutorUtils.class);

	private ExecutorUtils() {
	}

	public static ExecutorService newFixedSizeDaemonThreadPool(int threadPoolSize) {
		return Executors.newFixedThreadPool(threadPoolSize,
				new DaemonThreadFactory());
	}

	public static void shutdown(ExecutorService executor,
			TimeUnit softTimeoutUnits, long softTimeout,
			TimeUnit hardStopUnits, long hardStopTimeout) {
		executor.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!executor.awaitTermination(softTimeout, softTimeoutUnits)) {
				// Cancel currently executing tasks
				executor.shutdownNow();
				// Wait a while for tasks to respond to being canceled
				if (!executor.awaitTermination(hardStopTimeout, hardStopUnits))
					log.error("Pool did not terminate within timeout");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}
}
