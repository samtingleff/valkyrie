package com.rubiconproject.oss.kv.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.gen.Constants;
import com.rubiconproject.oss.kv.gen.GetResult;
import com.rubiconproject.oss.kv.gen.KeyValueService;
import com.rubiconproject.oss.kv.gen.KeyValueStoreException;
import com.rubiconproject.oss.kv.gen.KeyValueStoreIOException;
import com.rubiconproject.oss.kv.transcoder.ByteArrayTranscoder;
import com.rubiconproject.oss.kv.transcoder.Transcoder;

public class ThriftKeyValueServer {

	private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

	private Log log = LogFactory.getLog(getClass());

	private TServer server;

	private KeyValueStore backend;

	private int minWorkerThreads = 5;

	private int maxWorkerThreads = 500;

	public ThriftKeyValueServer() {
	}

	public ThriftKeyValueServer(KeyValueStore backend) {
		this.backend = backend;
	}

	public void setBackend(KeyValueStore backend) {
		this.backend = backend;
	}

	public void setMinWorkerThreads(int minWorkerThreads) {
		this.minWorkerThreads = minWorkerThreads;
	}

	public void setMaxWorkerThreads(int maxWorkerThreads) {
		this.maxWorkerThreads = maxWorkerThreads;
	}

	public void start() throws IOException {
		log.trace("start()");
		try {
			KeyValueStoreServiceHandler handler = new KeyValueStoreServiceHandler(
					backend);
			KeyValueService.Processor processor = new KeyValueService.Processor(
					handler);

			TServerSocket serverTransport = new TServerSocket(
					Constants.DEFAULT_PORT);
			TTransportFactory tfactory = new TTransportFactory();

			TProtocolFactory pfactory = new TBinaryProtocol.Factory();
			// this.server = new TThreadPoolServer(processor, serverTransport,
			// tfactory, pfactory);
			TTransportFactory ttfactory = new TFramedTransport.Factory();
			
			/*TThreadPoolServer.Options options = new TThreadPoolServer.Options();
			options.minWorkerThreads = minWorkerThreads;
			options.maxWorkerThreads = maxWorkerThreads;

			this.server = new TThreadPoolServer(processor, serverTransport,
					ttfactory, ttfactory, pfactory, pfactory, options);

			Thread t = new Thread(new Runnable() {
				public void run() {
					server.serve();
				}
			}, "KeyValueService");
			t.setDaemon(true);
			t.start();*/
		} catch (TTransportException e) {
			log.error("TTransportException inside start()", e);
			throw new IOException(e);
		} finally {
		}
	}

	private static class KeyValueStoreServiceHandler implements
			KeyValueService.Iface {
		private Log log = LogFactory.getLog(getClass());

		private Log accessLog = LogFactory.getLog("haymitch.thrift.accesslog");

		private Transcoder transcoder = new ByteArrayTranscoder();

		private KeyValueStore backend;

		public KeyValueStoreServiceHandler(KeyValueStore backend) {
			this.backend = backend;
		}

		public boolean exists(String key) throws KeyValueStoreIOException,
				KeyValueStoreException, TException {
			log.trace("exists()");
			long start = System.currentTimeMillis();
			boolean success = false;
			try {
				boolean b = backend.exists(key);
				success = true;
				return b;
			} catch (com.rubiconproject.oss.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside exists()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside exists()", e);
				throw new KeyValueStoreIOException();
			} finally {
				if (accessLog.isInfoEnabled()) {
					long time = System.currentTimeMillis() - start;
					accessLog.info(String.format("exists %1$s %2$d 0 %3$s",
							key, time, success));
				}
			}
		}

		public GetResult getValue(String key) throws KeyValueStoreIOException,
				KeyValueStoreException, TException {
			log.trace("getValue()");
			long start = System.currentTimeMillis();
			long byteCount = 0;
			boolean success = false;
			try {
				Object obj = backend.get(key, transcoder);
				GetResult result = null;
				if (obj == null) {
					result = new GetResult(false, EMPTY_BYTE_BUFFER);
				} else {
					byte[] bytes = (byte[]) obj;
					byteCount = bytes.length;
					result = new GetResult(true, ByteBuffer.wrap(bytes));
				}
				success = true;
				return result;
			} catch (com.rubiconproject.oss.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside getValue()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside getValue()", e);
				throw new KeyValueStoreIOException();
			} finally {
				if (accessLog.isInfoEnabled()) {
					long time = System.currentTimeMillis() - start;
					accessLog.info(String.format("get %1$s %2$d %3$d %4$s",
							key, time, byteCount, success));
				}
			}
		}

		public Map<String, GetResult> getBulk(List<String> keys)
				throws KeyValueStoreIOException, KeyValueStoreException,
				TException {
			log.trace("getValue()");
			long start = System.currentTimeMillis();
			long byteCount = 0;
			boolean success = false;
			try {
				Map<String, Object> backendResult = backend.getBulk(keys,
						transcoder);
				Map<String, GetResult> results = new HashMap<String, GetResult>(
						backendResult.size());
				for (Map.Entry<String, Object> entry : backendResult.entrySet()) {
					GetResult result = new GetResult(true, ByteBuffer.wrap((byte[]) entry
							.getValue()));
					results.put(entry.getKey(), result);
				}
				success = true;
				return results;
			} catch (com.rubiconproject.oss.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside getValue()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside getValue()", e);
				throw new KeyValueStoreIOException();
			} finally {
				if (accessLog.isInfoEnabled()) {
					long time = System.currentTimeMillis() - start;
					accessLog.info(String.format("getbulk %1$s %2$d %3$d %4$s",
							"_", time, byteCount, success));
				}
			}
		}

		public void setValue(String key, ByteBuffer data)
				throws KeyValueStoreIOException, KeyValueStoreException,
				TException {
			log.trace("setValue()");
			long start = System.currentTimeMillis();
			long byteCount = 0;
			boolean success = false;
			try {
				byte[] bytes = data.array();
				backend.set(key, bytes, transcoder);
				byteCount = bytes.length;
				success = true;
			} catch (com.rubiconproject.oss.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside setValue()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside setValue()", e);
				throw new KeyValueStoreIOException();
			} finally {
				if (accessLog.isInfoEnabled()) {
					long time = System.currentTimeMillis() - start;
					accessLog.info(String.format("set %1$s %2$d %3$d %4$s",
							key, time, byteCount, success));
				}
			}
		}

		public void deleteValue(String key) throws KeyValueStoreIOException,
				KeyValueStoreException, TException {
			log.trace("deleteValue()");
			long start = System.currentTimeMillis();
			boolean success = false;
			try {
				backend.delete(key);
				success = true;
			} catch (com.rubiconproject.oss.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside deleteValue()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside deleteValue()", e);
				throw new KeyValueStoreIOException();
			} finally {
				if (accessLog.isInfoEnabled()) {
					long time = System.currentTimeMillis() - start;
					accessLog.info(String.format("delete %1$s %2$d 0 %3$s",
							key, time, success));
				}
			}
		}
	}
}
