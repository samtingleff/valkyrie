package com.othersonline.kv.backends;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.util.DaemonThreadFactory;

public class ReplicatingKeyValueStore extends BaseManagedKeyValueStore {

	public static final String IDENTIFIER = "replicating";

	private Log log = LogFactory.getLog(getClass());

	private ExecutorService executor;

	private Random random = new Random();

	private KeyValueStore master;

	private List<KeyValueStore> replicas;

	private int threadPoolSize = 1;

	private boolean iOwnThreadPool = true;

	public ReplicatingKeyValueStore() {
		this.replicas = new ArrayList<KeyValueStore>();
	}

	public ReplicatingKeyValueStore(KeyValueStore master) {
		this.replicas = new ArrayList<KeyValueStore>();
		this.master = master;
	}

	public ReplicatingKeyValueStore(KeyValueStore master,
			List<KeyValueStore> replicas) {
		this.master = master;
		this.replicas = replicas;
		this.threadPoolSize = replicas.size();
	}

	public ReplicatingKeyValueStore(KeyValueStore master,
			List<KeyValueStore> replicas, int threadPoolSize) {
		this.master = master;
		this.replicas = replicas;
		this.threadPoolSize = threadPoolSize;
	}

	public ReplicatingKeyValueStore(KeyValueStore master,
			List<KeyValueStore> replicas, ExecutorService executor) {
		this.master = master;
		this.replicas = replicas;
		this.executor = executor;
	}

	public void setMaster(KeyValueStore master) {
		this.master = master;
	}

	public void setReplicas(List<KeyValueStore> replicas) {
		this.replicas = replicas;
	}

	public void setExecutorService(ExecutorService executor) {
		this.executor = executor;
	}

	public void addReplica(KeyValueStore replica) {
		replicas.add(replica);
	}

	public void removeReplica(KeyValueStore replica) {
		replicas.remove(replica);
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void start() throws IOException {
		if (executor == null) {
			executor = Executors.newFixedThreadPool(threadPoolSize,
					new DaemonThreadFactory());
			iOwnThreadPool = true;
		} else
			iOwnThreadPool = false;
		super.start();
	}

	@Override
	public void stop() {
		if (iOwnThreadPool) {
			executor.shutdown(); // Disable new tasks from being submitted
			try {
				// Wait a while for existing tasks to terminate
				if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
					// Cancel currently executing tasks
					executor.shutdownNow();
					// Wait a while for tasks to respond to being cancelled
					if (!executor.awaitTermination(2, TimeUnit.SECONDS))
						log.error("Pool did not terminate within timeout");
				}
			} catch (InterruptedException ie) {
				// (Re-)Cancel if current thread also interrupted
				executor.shutdownNow();
				// Preserve interrupt status
				Thread.currentThread().interrupt();
			}
			executor = null;
		}
		super.stop();
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		boolean exists = false;
		try {
			exists = getReadReplica().exists(key);
		} catch (Exception e) {
			exists = master.exists(key);
		}
		return exists;
	}

	@Override
	public Object get(String key) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		assertReadable();
		Object obj = null;
		try {
			obj = getReadReplica().get(key);
		} catch (Exception e) {
			obj = master.get(key);
		}
		return obj;
	}

	@Override
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		Object obj = null;
		try {
			obj = getReadReplica().get(key, transcoder);
		} catch (Exception e) {
			obj = master.get(key, transcoder);
		}
		return obj;
	}

	@Override
	public void set(String key, Serializable value)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		master.set(key, value);
		replicateWrite(key, value, null);
	}

	@Override
	public void set(String key, Serializable value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		master.set(key, value, transcoder);
		replicateWrite(key, value, transcoder);
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		master.delete(key);
		replicateDelete(key);
	}

	private KeyValueStore getReadReplica() {
		int size = replicas.size();
		if (size == 0)
			return master;
		else {
			int index = random.nextInt(size);
			return replicas.get(index);
		}
	}

	private void replicateWrite(String key, Serializable value,
			Transcoder transcoder) {
		for (KeyValueStore replica : replicas) {
			WriteReplicaRunnable runner = new WriteReplicaRunnable(replica,
					key, value, transcoder);
			executor.execute(runner);
		}
	}

	private void replicateDelete(String key) {
		for (KeyValueStore replica : replicas) {
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

		@Override
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
