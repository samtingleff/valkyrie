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
		NodeStore store = new DummyNodeStore(createNodeList(10, 3, 10));
		KetamaNodeLocator locator = new KetamaNodeLocator();
		locator.setActiveNodes(store.getActiveNodes());
		store.addChangeListener(locator);
		testPhysicalNodeKeyDistribution(locator, new KetamaHashAlgorithm());
	}

	public void testDynamoNodeLocator() {
		int physicalHosts = 10, nodesPerHost = 3, logicalsPerNode = 100;
		NodeStore store = new DummyNodeStore(createNodeList(physicalHosts,
				nodesPerHost, logicalsPerNode));
		DynamoNodeLocator locator = new DynamoNodeLocator();
		locator.setActiveNodes(store.getActiveNodes());
		store.addChangeListener(locator);
		HashAlgorithm hashAlg = new MD5HashAlgorithm();

		testPhysicalNodeKeyDistribution(locator, hashAlg);
		testTokenKeyDistribution(locator, hashAlg, physicalHosts * nodesPerHost
				* logicalsPerNode);
	}

	private void testTokenKeyDistribution(NodeLocator nodeLocator,
			HashAlgorithm hashAlg, int nodeCount) {
		Random random = new Random();
		// array to count key assignments
		int[] keyAssignments = new int[nodeCount];
		for (int i = 0; i < 100000; ++i) {
			String key = String.format("/blobs/users/%1$d/%2$d/%3$d", random
					.nextInt(100), random.nextInt(10000), random
					.nextInt(Integer.MAX_VALUE));

			int primary = nodeLocator.getPrimaryNode(hashAlg, key);
			++keyAssignments[primary];
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
	}

	private void testPhysicalNodeKeyDistribution(NodeLocator nodeLocator,
			HashAlgorithm hashAlg) {
		Random random = new Random();
		// array to count key assignments
		int[] keyAssignments = new int[10 * 3];
		for (int i = 0; i < 100000; ++i) {
			String key = String.format("/blobs/users/%1$d/%2$d/%3$d", random
					.nextInt(100), random.nextInt(10000), random
					.nextInt(Integer.MAX_VALUE));
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
		assertEquals(stats.getMean(), 10000.0d);
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
				addNode(results, nodeId, i, String.format("salt:%1$d:%2$d", i,
						j), String.format("uri://host#%1$s:%2$d", i, r
						.nextInt(1024)), logicals);
				++nodeId;
			}
		}
		return results;
	}

	private void addNode(List<Node> nodes, int nodeId, int physicalId,
			String salt, String uri, List<Integer> logicals) {
		Node n = new DefaultNodeImpl(nodeId, physicalId, salt, uri, logicals);
		nodes.add(n);
	}
}
