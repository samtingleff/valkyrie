package com.othersonline.kv.distributed.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeChangeListener;
import com.othersonline.kv.distributed.NodeLocator;
import com.othersonline.kv.distributed.hashing.HashAlgorithm;
import com.othersonline.kv.distributed.hashing.HashRing;
import com.othersonline.kv.distributed.hashing.KetamaHashAlgorithm;

/**
 * Modified from the spy memcached client, which has the following license.
 * 
 * This is roughly comparable to partitioning Strategy 1 in dynamo. Read the
 * notes in that paper.
 * 
 * Copyright (c) 2006-2009 Dustin Sallings <dustin@spy.net>
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 * @author Dustin Sallings <dustin@spy.net>
 * @author Sam Tingleff <sam@tingleff.com>
 */
public class KetamaNodeLocator implements NodeLocator, NodeChangeListener {
	private static final int NUM_REPS = 160;

	private KetamaHashAlgorithm hashAlg = new KetamaHashAlgorithm();

	private volatile HashRing<Long, Node> ketamaNodes = null;

	public KetamaNodeLocator() {
	}

	public void setActiveNodes(List<Node> nodes) {
		ketamaNodes = build(nodes);
	}

	public int getPrimaryNode(HashAlgorithm hashAlg, String key) {
		return 0;
	}

	public List<Node> getPreferenceList(final HashAlgorithm hashAlg,
			final String key, int count) {
		Iterator<Node> iter = new KetamaIterator(ketamaNodes, key, ketamaNodes
				.size());
		List<Node> results = new ArrayList<Node>(count);
		while ((results.size() < count) && (iter.hasNext())) {
			Node n = iter.next();
			if (!results.contains(n))
				results.add(n);
		}
		return results;
	}

	public List<Node> getFullPreferenceList(HashAlgorithm hashAlg, String key) {
		return getPreferenceList(hashAlg, key, ketamaNodes.size());
	}

	private HashRing<Long, Node> build(List<Node> nodes) {
		HashRing<Long, Node> ketamaNodes = new HashRing<Long, Node>(nodes
				.size());
		for (Node node : nodes) {
			String nodeIdentifier = node.getSalt();
			for (int i = 0; i < NUM_REPS / 4; ++i) {
				byte[] digest = hashAlg.md5(nodeIdentifier + "-" + i);
				for (int h = 0; h < 4; h++) {
					Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
							| ((long) (digest[2 + h * 4] & 0xFF) << 16)
							| ((long) (digest[1 + h * 4] & 0xFF) << 8)
							| (digest[h * 4] & 0xFF);
					ketamaNodes.put(k, node);
				}
			}
		}
		return ketamaNodes;
	}

	private Node getNodeForKey(final HashRing<Long, Node> ketamaNodes, long hash) {
		Node rv = null;
		// dsallings:
		// "Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
		// in a lot of places, so I'm doing this myself."
		// sam: I'm not. modified to use jdk 1.6 ceilingEntry in the HashRing class
		Map.Entry<Long, Node> entry = ketamaNodes.place(hash);
		if (entry != null)
			rv = entry.getValue();
		assert rv != null : "Found no node for hash " + hash;
		return rv;
	}

	class KetamaIterator implements Iterator<Node> {

		private HashRing<Long, Node> iteratorNodes;

		final String key;

		long hashVal;

		int remainingTries;

		int numTries = 0;

		public KetamaIterator(final HashRing<Long, Node> nodes, final String k,
				final int t) {
			super();
			iteratorNodes = nodes;
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
				return getNodeForKey(iteratorNodes, hashVal);
			} finally {
				nextHash();
			}
		}

		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}
	}
}
