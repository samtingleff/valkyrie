package com.othersonline.kv.distributed.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import com.othersonline.kv.distributed.HashAlgorithm;
import com.othersonline.kv.distributed.Node;
import com.othersonline.kv.distributed.NodeLocator;
import com.othersonline.kv.distributed.NodeStore;
import com.othersonline.kv.distributed.impl.DefaultNodeImpl;
import com.othersonline.kv.distributed.impl.DynamoNodeLocator;
import com.othersonline.kv.distributed.impl.KetamaHashAlgorithm;
import com.othersonline.kv.distributed.impl.KetamaNodeLocator;
import com.othersonline.kv.distributed.impl.MD5HashAlgorithm;

public class NodeLocatorTestCase extends TestCase {
	public void testKetamaNodeLocator() {
		testKeyDistribution(new KetamaNodeLocator(new DummyNodeStore(
				createNodeList(10, 3, 10))), new KetamaHashAlgorithm());
	}

	public void testDynamoNodeLocator() {
		testKeyDistribution(new DynamoNodeLocator(new DummyNodeStore(
				createNodeList(10, 3, 10))), new MD5HashAlgorithm());
	}

	private void testFailureResiliance(NodeStore nodeStore, NodeLocator nodeLocator, HashAlgorithm hashAlg) {
		int numKeys = 10;
		Random random = new Random();
		List<String> keys = new ArrayList<String>(numKeys);
		for (int i = 0; i < numKeys; ++i) {
			String key = String.format("/blobs/users/%1$d/%2$d/%3$d", random
					.nextInt(100), random.nextInt(10000), random
					.nextInt(Integer.MAX_VALUE));
			
			long hashCode = hashAlg.hash(key);
			List<Node> nodeList = nodeLocator
			.getPreferenceList(hashAlg, key, 3);
			for (Node node : nodeList) {
				// set some value
				
			}
			keys.add(key);
	
		}
		
	}
	private void testKeyDistribution(NodeLocator nodeLocator, HashAlgorithm hashAlg) {
		Random random = new Random();
		// array to count key assignments
		int[] keyAssignments = new int[10 * 3];
		for (int i = 0; i < 100000; ++i) {
			String key = String.format("/blobs/users/%1$d/%2$d/%3$d", random
					.nextInt(100), random.nextInt(10000), random
					.nextInt(Integer.MAX_VALUE));
			long hashCode = hashAlg.hash(key);

			List<Node> nodeList = nodeLocator
					.getPreferenceList(hashAlg, key, 3);
			for (Node node : nodeList) {
				++keyAssignments[node.getId() - 1];
			}
		}

		DescriptiveStatistics stats = new DescriptiveStatistics();
		for (int i = 0; i < keyAssignments.length; ++i) {
			stats.addValue((double) keyAssignments[i]);
		}
		System.out.println("min:      " + stats.getMin());
		System.out.println("max:      " + stats.getMax());
		System.out.println("avg:      " + stats.getMean());
		System.out.println("stdev:    " + stats.getStandardDeviation());
		System.out.println("variance: " + stats.getVariance());
		assertTrue(stats.getStandardDeviation() <= 4000);
	}

	private List<Node> createNodeList(int physicalHosts, int nodesPerPhysical,
			int logicalsPerPhysical) {
		Random r = new Random();
		List<Node> results = new ArrayList<Node>(physicalHosts
				* nodesPerPhysical);
		int nodeId = 1, counter = 1;
		for (int i = 1; i <= physicalHosts; ++i) {
			for (int j = 0; j < nodesPerPhysical; ++j) {
				List<Integer> logicals = new ArrayList<Integer>(
						logicalsPerPhysical);
				for (int k = 1; k <= logicalsPerPhysical; ++k) {
					logicals.add(new Integer(20 * counter));
					++counter;
				}
				Node n = new DefaultNodeImpl(nodeId, i, String.format(
						"salt:%1$d:%2$d", i, j), String.format(
						"uri://host#%1$s:%2$d", i, r.nextInt(1024)), logicals);
				results.add(n);
				++nodeId;
			}
		}
		return results;
	}
}
