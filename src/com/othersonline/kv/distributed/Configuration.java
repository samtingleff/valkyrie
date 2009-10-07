package com.othersonline.kv.distributed;

import java.util.concurrent.TimeUnit;

import com.othersonline.kv.backends.ConnectionFactory;

public class Configuration {
	private NodeStore nodeStore;

	private OperationQueue syncOperationQueue;

	private OperationQueue asyncOperationQueue;

	private ConnectionFactory connectionFactory;

	// r: minimum # of nodes that must participate in a successful read
	private int requiredReads = 2;

	// w: minimum # of nodes that must participate in a successful write
	private int requiredWrites = 2;

	// n: number of write replicas. r + w should be > n
	private int writeReplicas = 3;

	// number of read replicas. provided as a tuning parameter for high perf
	// reads (set low).
	private int readReplicas = writeReplicas;

	// number of reads required for an unambiguous null response
	// currently not used within the framework
	private int unambiguousReadCount = readReplicas;

	// max wait time for reads
	private long readOperationTimeout = 1000l;

	// max wait time for writes
	private long writeOperationTimeout = 1000l;

	// max errors in a given period before a node is evicted
	private int maxNodeErrorCount = 10;

	// time period for above error count
	private TimeUnit nodeErrorCountPeriod = TimeUnit.MINUTES;

	// send an async set() request to nodes that return null on get requests
	private boolean fillNullGetResults = true;

	// send an async set() request to nodes that throw an exception on get requests
	private boolean fillErrorGetResults = false;

	public NodeStore getNodeStore() {
		return nodeStore;
	}

	public void setNodeStore(NodeStore store) {
		this.nodeStore = store;
	}

	public OperationQueue getSyncOperationQueue() {
		return syncOperationQueue;
	}

	public void setSyncOperationQueue(OperationQueue queue) {
		this.syncOperationQueue = queue;
	}

	public OperationQueue getAsyncOperationQueue() {
		return asyncOperationQueue;
	}

	public void setAsyncOperationQueue(OperationQueue queue) {
		this.asyncOperationQueue = queue;
	}

	public ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public void setConnectionFactory(ConnectionFactory factory) {
		this.connectionFactory = factory;
	}

	public int getRequiredReads() {
		return requiredReads;
	}

	public void setRequiredReads(int requiredReads) {
		this.requiredReads = requiredReads;
	}

	public int getRequiredWrites() {
		return requiredWrites;
	}

	public void setRequiredWrites(int requiredWrites) {
		this.requiredWrites = requiredWrites;
	}

	public int getWriteReplicas() {
		return writeReplicas;
	}

	public void setWriteReplicas(int writeReplicas) {
		this.writeReplicas = writeReplicas;
	}

	public int getReadReplicas() {
		return readReplicas;
	}

	public void setReadReplicas(int readReplicas) {
		this.readReplicas = readReplicas;
	}

	public int getUnambiguousReadCount() {
		return unambiguousReadCount;
	}

	public void setUnambiguousReadCount(int unambiguousReadCount) {
		this.unambiguousReadCount = unambiguousReadCount;
	}

	public long getReadOperationTimeout() {
		return readOperationTimeout;
	}

	public void setReadOperationTimeout(long readOperationTimeout) {
		this.readOperationTimeout = readOperationTimeout;
	}

	public long getWriteOperationTimeout() {
		return writeOperationTimeout;
	}

	public void setWriteOperationTimeout(long writeOperationTimeout) {
		this.writeOperationTimeout = writeOperationTimeout;
	}

	public int getMaxNodeErrorCount() {
		return maxNodeErrorCount;
	}

	public void setMaxNodeErrorCount(int maxNodeErrorCount) {
		this.maxNodeErrorCount = maxNodeErrorCount;
	}

	public TimeUnit getNodeErrorCountPeriod() {
		return nodeErrorCountPeriod;
	}

	public void setNodeErrorCountPeriod(TimeUnit nodeErrorCountPeriod) {
		this.nodeErrorCountPeriod = nodeErrorCountPeriod;
	}

	public boolean getFillNullGetResults() {
		return fillNullGetResults;
	}

	public void setFillNullGetResults(boolean fillNullGetResults) {
		this.fillNullGetResults = fillNullGetResults;
	}

	public boolean getFillErrorGetResults() {
		return fillErrorGetResults;
	}

	public void setFillErrorGetResults(boolean fillErrorGetResults) {
		this.fillErrorGetResults = fillErrorGetResults;
	}
}
