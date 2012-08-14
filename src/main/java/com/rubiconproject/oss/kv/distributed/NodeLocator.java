package com.rubiconproject.oss.kv.distributed;

import java.util.List;

import com.rubiconproject.oss.kv.distributed.hashing.HashAlgorithm;

public interface NodeLocator {
	public void setActiveNodes(List<Node> nodes);

	public int getPrimaryNode(HashAlgorithm hashAlg, String key);

	public List<Node> getPreferenceList(final HashAlgorithm hashAlg,
			final String key, final int count);

	List<Node> getFullPreferenceList(HashAlgorithm hashAlg, String key);
}
