package com.rubiconproject.oss.kv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rubiconproject.oss.kv.transcoder.Transcoder;
import com.rubiconproject.oss.kv.util.DaemonThreadFactory;

public class ThreadPoolAsyncFlushQueue implements AsyncFlushQueue {
	private ExecutorService executor;

	private KeyValueStore store;

	public ThreadPoolAsyncFlushQueue() {
	}

	public ThreadPoolAsyncFlushQueue(KeyValueStore store, int threadPoolSize) {
		this.store = store;
		executor = Executors.newFixedThreadPool(threadPoolSize,
				new DaemonThreadFactory());
	}

	public void set(String key, Object value) {
		executor.submit(new SetRunnable(store, key, value));
	}

	public void set(String key, Object value, Transcoder transcoder) {
		executor.submit(new SetTranscoderRunnable(store, key, value, transcoder));
	}

	public void delete(String key) {
		executor.submit(new DeleteRunnable(store, key));
	}

	private static abstract class QueueRunnable implements Runnable {
		protected Log log = LogFactory.getLog(ThreadPoolAsyncFlushQueue.class);

		protected KeyValueStore store;

		public QueueRunnable(KeyValueStore store) {
			this.store = store;
		}
	}

	private static class SetRunnable extends QueueRunnable implements Runnable {
		protected String key;

		protected Object value;

		public SetRunnable(KeyValueStore store, String key, Object value) {
			super(store);
			this.key = key;
			this.value = value;
		}

		public void run() {
			try {
				store.set(key, value);
			} catch (Exception e) {
				log.error("Exception calling set()", e);
			}
		}
	}

	private static class SetTranscoderRunnable extends SetRunnable implements
			Runnable {
		protected Transcoder transcoder;

		public SetTranscoderRunnable(KeyValueStore store, String key,
				Object value, Transcoder transcoder) {
			super(store, key, value);
			this.transcoder = transcoder;
		}

		public void run() {
			try {
				store.set(key, value, transcoder);
			} catch (Exception e) {
				log.error("Exception calling set()", e);
			}
		}
	}

	private static class DeleteRunnable extends QueueRunnable implements
			Runnable {
		protected String key;

		public DeleteRunnable(KeyValueStore store, String key) {
			super(store);
			this.key = key;
		}

		public void run() {
			try {
				store.delete(key);
			} catch (Exception e) {
				log.error("Exception calling delete()", e);
			}

		}
	}
}
