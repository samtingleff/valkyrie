package com.othersonline.kv.distributed.impl;

import java.util.List;

import com.othersonline.kv.distributed.Node;

public class DefaultNodeImpl implements Node, Comparable<Node> {
	private static final long serialVersionUID = 4061874101050720157L;

	private int id;

	private int physicalId;

	private String salt;

	private String connectionURI;

	private List<Integer> logicalPartitionList;

	public DefaultNodeImpl() {

	}

	public DefaultNodeImpl(int id, int physicalId, String salt,
			String connectionURI, List<Integer> logicalPartitions) {
		this.id = id;
		this.physicalId = physicalId;
		this.salt = salt;
		this.connectionURI = connectionURI;
		this.logicalPartitionList = logicalPartitions;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getPhysicalId() {
		return physicalId;
	}

	public void setPhysicalId(int physicalId) {
		this.physicalId = physicalId;
	}

	public String getSalt() {
		return salt;
	}

	public void setSalt(String salt) {
		this.salt = salt;
	}

	public String getConnectionURI() {
		return connectionURI;
	}

	public void setConnectionURI(String connectionURI) {
		this.connectionURI = connectionURI;
	}

	public List<Integer> getLogicalPartitionList() {
		return logicalPartitionList;
	}

	public void setLogicalPartitionList(List<Integer> logicalPartitionList) {
		this.logicalPartitionList = logicalPartitionList;
	}

	public boolean equals(Object obj) {
		return (this.compareTo(((Node) obj)) == 0);
	}

	public int compareTo(Node o) {
		return new Integer(this.id).compareTo(new Integer(o.getId()));
	}

}
