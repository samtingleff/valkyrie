package com.othersonline.kv.backends;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.util.DaemonThreadFactory;
import com.othersonline.kv.util.ExecutorUtils;

public class ReplicatingKeyValueStore extends ReadLoadBalancingKeyValueStore {

	public static final String IDENTIFIER = "replicating";

	private Log log = LogFactory.getLog(getClass());

	private ExecutorService executor;

	private int threadPoolSize = 1;

	private boolean iOwnThreadPool = true;

	public ReplicatingKeyValueStore() {
		super();
	}

	public ReplicatingKeyValueStore(KeyValueStore master) {
		super(master);
	}

	public ReplicatingKeyValueStore(KeyValueStore master,
			List<KeyValueStore> replicas) {
		super(master, replicas);
		this.threadPoolSize = replicas.size();
	}

	public ReplicatingKeyValueStore(KeyValueStore master,
			List<KeyValueStore> replicas, int threadPoolSize) {
		super(master, replicas);
		this.threadPoolSize = threadPoolSize;
	}

	public ReplicatingKeyValueStore(KeyValueStore master,
			List<KeyValueStore> replicas, ExecutorService executor) {
		super(master, replicas);
		this.executor = executor;
	}

	public void setExecutorService(ExecutorService executor) {
		this.executor = executor;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void addReplica(KeyValueStore store) {
		super.addReader(store);
	}

	public void removeReplica(KeyValueStore store) {
		super.removeReader(store);
	}

	public void start() throws IOException {
		if (executor == null) {
			executor = ExecutorUtils.newFixedSizeDaemonThreadPool(threadPoolSize);
			iOwnThreadPool = true;
		} else
			iOwnThreadPool = false;
		super.start();
	}

	public void stop() {
		if (iOwnThreadPool) {
			ExecutorUtils.shutdown(executor, TimeUnit.SECONDS, 2l, TimeUnit.SECONDS, 2);
			executor = null;
		}
		super.stop();
	}

	public void set(String key, Serializable value)
			throws KeyValueStoreException, IOException {
		super.set(key, value);
		replicateWrite(key, value, null);
	}

	public void set(String key, Serializable value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		super.set(key, value, transcoder);
		replicateWrite(key, value, transcoder);
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		super.delete(key);
		replicateDelete(key);
	}

	private void replicateWrite(String key, Serializable value,
			Transcoder transcoder) {
		for (KeyValueStore replica : readers) {
			WriteReplicaRunnable runner = new WriteReplicaRunnable(replica,
					key, value, transcoder);
			executor.execute(runner);
		}
	}

	private void replicateDelete(String key) {
		for (KeyValueStore replica : readers) {
			DeleteReplicaRunnable runner = new DeleteReplicaRunnable(replica,
					key);
			executor.execute(runner);
		}
	}

	private static class WriteReplicaRunnable implements Runnable {
		protected static Log log = LogFactory
				.getLog(WriteReplicaRunnable.class);

		protected KeyValueStore store;

		protected String key;

		protected Serializable value;

		protected Transcoder transcoder;

		public WriteReplicaRunnable(KeyValueStore store, String key,
				Serializable value, Transcoder transcoder) {
			this.store = store;
			this.key = key;
			this.value = value;
			this.transcoder = transcoder;
		}

		public void run() {
			try {
				if (transcoder == null)
					store.set(key, value);
				else
					store.set(key, value, transcoder);
			} catch (KeyValueStoreException e) {
				log.error("Cannot write to replica thread", e);
			} catch (IOException e) {
				log.error("Cannot write to replica thread", e);
			} finally {
			}
		}
	}

	private static class DeleteReplicaRunnable extends WriteReplicaRunnable {
		public DeleteReplicaRunnable(KeyValueStore store, String key) {
			super(store, key, null, null);
		}

		public void run() {
			try {
				store.delete(key);
			} catch (KeyValueStoreException e) {
				log.error("Cannot write to replica thread", e);
			} catch (IOException e) {
				log.error("Cannot write to replica thread", e);
			} finally {
			}
		}

	}
}
