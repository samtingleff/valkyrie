package com.othersonline.kv.test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.backends.FileSystemKeyValueStore;
import com.othersonline.kv.backends.HashtableKeyValueStore;
import com.othersonline.kv.backends.MemcachedKeyValueStore;
import com.othersonline.kv.backends.OsCacheKeyValueStore;
import com.othersonline.kv.backends.TokyoTyrantKeyValueStore;
import com.othersonline.kv.backends.WebDAVKeyValueStore;

import junit.framework.TestCase;

public class BenchmarkTestCase extends TestCase {

	public void testBenchmark() throws Exception {
		KeyValueStore[] backends = new KeyValueStore[] {
				new FileSystemKeyValueStore("tmp/fs"),
				new HashtableKeyValueStore(),
				new MemcachedKeyValueStore("stanley:11211"),
				new OsCacheKeyValueStore(),
				// new TokyoTyrantKeyValueStore("stanley", 1978), // croaks with > 1 thread
				new WebDAVKeyValueStore("http://stanley/dav/testing") };
		for (KeyValueStore kv : backends) {
			kv.start();
			TestResult tr = doTestStorageBackend(kv, 10, 100);
			System.out.println(String.format("%1$s,%2$d,%3$d", tr
					.getIdentifier(), tr.getDuration(), tr.getErrorCount()));
		}
	}

	private TestResult doTestStorageBackend(KeyValueStore store,
			int concurrency, int repetitions) throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(concurrency);
		List<Future<TestResult>> futures = new ArrayList<Future<TestResult>>(
				concurrency);
		for (int i = 0; i < concurrency; ++i) {
			Callable<TestResult> c = new Callable<TestResult>() {
				private KeyValueStore store;

				private int repetitions = 1;

				public Callable init(KeyValueStore store, int repetitions) {
					this.store = store;
					this.repetitions = repetitions;
					return this;
				}

				public TestResult call() {
					TestResult result = new TestResult(store.getIdentifier());
					long start = System.currentTimeMillis();
					for (int i = 0; i < repetitions; ++i) {
						try {
							MySerializableClass msc = new MySerializableClass(
									123, i, "string goes here",
									new MySerializableSubclass(12312.21d,
											12312343l, "another string here"));
							String key = String.format("/some.key.%1$d", i);
							store.set(key, msc);
							MySerializableClass fetch = (MySerializableClass) store
									.get(key);
							store.delete(key);
						} catch (Exception e) {
							result.addError();
						}
					}
					result.setDuration(System.currentTimeMillis() - start);
					return result;
				}
			}.init(store, repetitions);
			Future<TestResult> future = executor.submit(c);
			futures.add(future);
		}
		TestResult result = new TestResult(store.getIdentifier());
		for (Future<TestResult> future : futures) {
			TestResult tr = future.get(10l, TimeUnit.MINUTES);
			result.setDuration(result.getDuration() + tr.getDuration());
			result.addErrors(tr.getErrorCount());
		}
		return result;
	}

	private static class TestResult {
		private String identifier;

		private long duration;

		private int errorCount = 0;

		public TestResult(String identifier) {
			this.identifier = identifier;
		}

		public String getIdentifier() {
			return identifier;
		}

		public long getDuration() {
			return duration;
		}

		public void setDuration(long duration) {
			this.duration = duration;
		}

		public int getErrorCount() {
			return errorCount;
		}

		public void addError() {
			errorCount += 1;
		}

		public void addErrors(int errors) {
			errorCount += errors;
		}
	}

	private static class MySerializableClass implements Serializable,
			Externalizable {
		private int someInt = 0;

		private int someOtherInt = 0;

		private String someString;

		private MySerializableSubclass subobject;

		public MySerializableClass() {
		}

		public MySerializableClass(int someInt, int someOtherInt,
				String someString, MySerializableSubclass subobject) {
			this.someInt = someInt;
			this.someOtherInt = someOtherInt;
			this.someString = someString;
			this.subobject = subobject;
		}

		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			someInt = in.readInt();
			someOtherInt = in.readInt();
			someString = (String) in.readObject();
			subobject = (MySerializableSubclass) in.readObject();
		}

		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeInt(someInt);
			out.writeInt(someOtherInt);
			out.writeObject(someString);
			out.writeObject(subobject);
		}

	}

	private static class MySerializableSubclass implements Serializable,
			Externalizable {
		private double d = 0.0d;

		private long l = 123123l;

		private String s = null;

		public MySerializableSubclass() {
		}

		public MySerializableSubclass(double d, long l, String s) {
			this.d = d;
			this.l = l;
			this.s = s;
		}

		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			this.d = in.readDouble();
			this.l = in.readLong();
			this.s = (String) in.readObject();
		}

		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeDouble(d);
			out.writeLong(l);
			out.writeObject(s);
		}

	}
}
