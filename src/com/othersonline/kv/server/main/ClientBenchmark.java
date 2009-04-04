package com.othersonline.kv.server.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.springframework.context.support.AbstractApplicationContext;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.transcoder.GzippingTranscoder;
import com.othersonline.kv.transcoder.StringTranscoder;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.util.StreamUtils;

public class ClientBenchmark extends BaseKVServerMain {
	private static final String[] defaultClientSpringPaths = new String[] { "/com/othersonline/kv/server/applicationContext-benchmark.xml" };

	private Log log = LogFactory.getLog(getClass());

	@Option(name = "--backend", usage = "backend to test")
	private String backend;

	@Option(name = "--concurrency", usage = "# of concurrent threads (default is 10)")
	private int concurrency = 10;

	@Option(name = "--repetitions", usage = "repetitions per thread (default is 100)")
	private int repetitions = 100;

	@Option(name = "--gzip", usage = "use gzipping transcoder (default is false)")
	private boolean gzip = false;

	public static void main(String[] args) throws Exception {
		ClientBenchmark cb = new ClientBenchmark();
		int returnValue = cb.execute(args);
		System.exit(returnValue);
	}

	private int execute(String[] args) throws Exception {
		CmdLineParser parser = new CmdLineParser(this);
		parser.setUsageWidth(80);
		try {
			parser.parseArgument(args);
			AbstractApplicationContext ctx = getContext(defaultClientSpringPaths);
			KeyValueStore store = (KeyValueStore) ctx.getBean(backend);
			TestResult result = doTestStorageBackend(store, concurrency,
					repetitions);
			System.out.println("Backend\tDuration\tErrors");
			System.out.println(String.format("%1$s\t%2$d\t%3$d",
					result.identifier, result.duration, result.errorCount));
			return 0;
		} catch (CmdLineException e) {
			System.err.println("Usage: dshell [options...] arguments...");
			parser.printUsage(System.err);
			log.error("Could not parse options:", e);
			return 1;
		}
	}

	private TestResult doTestStorageBackend(KeyValueStore store,
			int concurrency, int repetitions) throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(concurrency);
		List<Future<TestResult>> futures = new ArrayList<Future<TestResult>>(
				concurrency);
		String content = StreamUtils
				.getResourceAsString("/com/othersonline/kv/test/resources/lorem-ipsum-10k.txt");
		for (int i = 0; i < concurrency; ++i) {
			Callable<TestResult> c = new Callable<TestResult>() {
				private Random random = new Random();

				private Transcoder stringTranscoder = new StringTranscoder();

				private Transcoder gzipTranscoder = new GzippingTranscoder(
						stringTranscoder);

				private KeyValueStore store;

				private int repetitions = 1;

				private String content;

				private boolean gzip = false;

				public Callable init(KeyValueStore store, int repetitions,
						String content, boolean gzip) {
					this.store = store;
					this.repetitions = repetitions;
					this.content = content;
					this.gzip = gzip;
					return this;
				}

				public TestResult call() {
					TestResult result = new TestResult(store.getIdentifier());
					long start = System.currentTimeMillis();
					for (int i = 0; i < repetitions; ++i) {
						try {
							Transcoder transcoder = (gzip) ? gzipTranscoder
									: stringTranscoder;
							String key = String.format(
									"/some.key.%1$d.%2$d.txt",
									random.nextInt(), i);
							store.set(key, content, transcoder);
							String s = (String) store.get(key, transcoder);
							store.delete(key);
						} catch (Exception e) {
							result.addError();
						}
					}
					result.setDuration(System.currentTimeMillis() - start);
					return result;
				}
			}.init(store, repetitions, content, gzip);
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
}
