package com.othersonline.kv.distributed.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.othersonline.kv.distributed.HashAlgorithm;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeChangeListener;
import com.othersonline.kv.distributed.NodeLocator;
import com.othersonline.kv.distributed.NodeStore;

public class DynamoNodeLocator implements NodeLocator, NodeChangeListener {
	private NodeStore nodeStore;

	private int tokensPerNode = 3;

	private HashAlgorithm hashAlg = new MD5HashAlgorithm();

	private int nodeCount = 0;

	private HashRing<Long, Token> outerRing = new HashRing<Long, Token>();

	private HashRing<Long, Node> innerRing = new HashRing<Long, Node>();

	public DynamoNodeLocator(NodeStore store) {
		setNodeStore(store);
	}

	public void setNodeStore(NodeStore store) {
		this.nodeStore = store;
		nodeStore.addChangeListener(this);
		rebuild(nodeStore.getActiveNodes());
	}

	public List<Node> getPreferenceList(HashAlgorithm hashAlg, String key,
			int count) {
		Iterator<Node> iter = new DynamoIterator(outerRing, innerRing, key,
				outerRing.size());
		List<Node> results = new ArrayList<Node>(count);
		while ((results.size() < count) && (iter.hasNext())) {
			Node n = iter.next();
			if (!results.contains(n))
				results.add(n);
		}
		return results;
	}

	public void activeNodes(List<Node> nodes) {
		rebuild(nodes);
	}

	private Node getNodeForKey(final HashRing<Long, Token> outer,
			final HashRing<Long, Node> inner, final long key) {
		Map.Entry<Long, Token> outerEntry = outer.place(key);
		Node n = inner.place(outerEntry.getValue().pointer).getValue();
		return n;
	}

	private void rebuild(List<Node> nodes) {
		nodeCount = nodes.size();
		// build the outer ring from Long.MIN_VALUE to Long.MAX_VALUE
		int tokens = tokensPerNode * nodeCount;
		long tokenSize = (Long.MAX_VALUE / tokens) * 2;
		for (int i = 1; i <= tokens; ++i) {
			long index = Long.MIN_VALUE + (i * tokenSize);
			Token token = new Token();
			outerRing.put(index, token);
		}
		for (Node node : nodes) {
			for (int i = 0; i < tokensPerNode; ++i) {
				// assign T tokens to this node
				String identifier = i + node.getSalt();
				long hashCode = hashAlg.hash(identifier);
				// place hashCode on inner ring
				place(hashCode, node, outerRing, innerRing);
			}
		}
	}

	private void place(long hashCode, Node node, HashRing<Long, Token> outer,
			HashRing<Long, Node> inner) {
		Map.Entry<Long, Token> ceilingEntry = outer.place(hashCode);
		Token t = ceilingEntry.getValue();
		while (t.pointer != null) {
			
			ceilingEntry = outer.higherEntry(ceilingEntry.getKey());
			if (ceilingEntry == null)
				ceilingEntry = outer.firstEntry();
			t = ceilingEntry.getValue();
		}
		t.pointer = hashCode;
		innerRing.put(hashCode, node);
	}

	private static class Token {
		public Long pointer;
	}

	class DynamoIterator implements Iterator<Node> {

		private HashRing<Long, Token> outer;

		private HashRing<Long, Node> inner;

		final String key;

		long hashVal;

		int remainingTries;

		int numTries = 0;

		public DynamoIterator(final HashRing<Long, Token> outer,
				final HashRing<Long, Node> inner, final String k, final int t) {
			super();
			this.outer = outer;
			this.inner = inner;
			hashVal = hashAlg.hash(k);
			remainingTries = t;
			key = k;
		}

		private void nextHash() {
			hashVal = hashAlg.hash((numTries++) + key);
			remainingTries--;
		}

		public boolean hasNext() {
			return remainingTries > 0;
		}

		public Node next() {
			try {
				return getNodeForKey(outer, inner, hashVal);
			} finally {
				nextHash();
			}
		}

		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}
	}
}
