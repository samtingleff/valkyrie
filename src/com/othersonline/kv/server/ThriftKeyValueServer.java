package com.othersonline.kv.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
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

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.gen.Constants;
import com.othersonline.kv.gen.GetResult;
import com.othersonline.kv.gen.KeyValueService;
import com.othersonline.kv.gen.KeyValueStoreException;
import com.othersonline.kv.gen.KeyValueStoreIOException;
import com.othersonline.kv.transcoder.ByteArrayTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class ThriftKeyValueServer {
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
			TThreadPoolServer.Options options = new TThreadPoolServer.Options();
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
			t.start();
		} catch (TTransportException e) {
			log.error("TTransportException inside start()", e);
			throw new IOException(e);
		} finally {
		}
	}

	private static class KeyValueStoreServiceHandler implements
			KeyValueService.Iface {
		private Log log = LogFactory.getLog(getClass());

		private Log accessLog = LogFactory.getLog("logging.access");

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
			} catch (com.othersonline.kv.KeyValueStoreException e) {
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
				if (obj == null)
					result = new GetResult(false, key, new byte[] {});
				else {
					byte[] bytes = (byte[]) obj;
					byteCount = bytes.length;
					result = new GetResult(true, key, bytes);
				}
				success = true;
				return result;
			} catch (com.othersonline.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside getValue()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside getValue()", e);
				throw new KeyValueStoreIOException();
			} catch (ClassNotFoundException e) {
				log.error("ClassNotFoundException inside getValue()", e);
				throw new KeyValueStoreException();
			} finally {
				if (accessLog.isInfoEnabled()) {
					long time = System.currentTimeMillis() - start;
					accessLog.info(String.format("get %1$s %2$d %3$d %4$s",
							key, time, byteCount, success));
				}
			}
		}

		public List<GetResult> getBulk(List<String> keys)
				throws KeyValueStoreIOException, KeyValueStoreException,
				TException {
			log.trace("getValue()");
			long start = System.currentTimeMillis();
			long byteCount = 0;
			boolean success = false;
			try {
				Map<String, Object> backendResult = backend.getBulk(keys,
						transcoder);
				List<GetResult> results = new ArrayList<GetResult>(
						backendResult.size());
				for (Map.Entry<String, Object> entry : backendResult.entrySet()) {

					GetResult result = new GetResult(true, entry.getKey(),
							(byte[]) entry.getValue());
					results.add(result);
				}
				success = true;
				return results;
			} catch (com.othersonline.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside getValue()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside getValue()", e);
				throw new KeyValueStoreIOException();
			} catch (ClassNotFoundException e) {
				log.error("ClassNotFoundException inside getValue()", e);
				throw new KeyValueStoreException();
			} finally {
				if (accessLog.isInfoEnabled()) {
					long time = System.currentTimeMillis() - start;
					accessLog.info(String.format("getbulk %1$s %2$d %3$d %4$s",
							"_", time, byteCount, success));
				}
			}
		}

		public void setValue(String key, byte[] data)
				throws KeyValueStoreIOException, KeyValueStoreException,
				TException {
			log.trace("setValue()");
			long start = System.currentTimeMillis();
			long byteCount = 0;
			boolean success = false;
			try {
				backend.set(key, data, transcoder);
				byteCount = data.length;
				success = true;
			} catch (com.othersonline.kv.KeyValueStoreException e) {
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
			} catch (com.othersonline.kv.KeyValueStoreException e) {
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
