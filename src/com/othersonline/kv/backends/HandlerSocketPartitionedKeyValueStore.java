package com.othersonline.kv.backends;

import java.util.concurrent.TimeoutException;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.backends.handlersocket.HSClient;
import com.othersonline.kv.backends.handlersocket.exception.HandlerSocketException;

public class HandlerSocketPartitionedKeyValueStore extends
		HandlerSocketKeyValueStore implements KeyValueStore {
	public static final String IDENTIFIER = "handlerSocketPartitioned";

	private int partitions = 16;

	@Configurable(name = "partitions", accepts = Type.IntType)
	public void setPartitions(int partitions) {
		this.partitions = partitions;
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	protected int getIndexId(HSClient client, String key) {
		return Math.abs(key.hashCode() % partitions);
	}

	@Override
	protected void openIndices(HSClient client) throws InterruptedException,
			TimeoutException, HandlerSocketException {
		for (int i = 0; i < partitions; ++i) {
			client.openIndex(i, db, getTable(i), index, serializer
					.valueColumns());
		}
	}

	protected String getTable(int i) {
		return String.format("%s_%03d", table, i);
	}
}
