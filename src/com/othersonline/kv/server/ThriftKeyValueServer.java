package com.othersonline.kv.server;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
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

	public ThriftKeyValueServer() {
	}

	public ThriftKeyValueServer(KeyValueStore backend) {
		this.backend = backend;
	}

	public void setBackend(KeyValueStore backend) {
		this.backend = backend;
	}

	public void start() throws IOException {
		log.trace("start()");
		try {
			KeyValueStoreServiceHandler handler = new KeyValueStoreServiceHandler(
					backend);
			KeyValueService.Processor processor = new KeyValueService.Processor(
					handler);

			TServerTransport serverTransport = new TServerSocket(
					Constants.DEFAULT_PORT);
			TTransportFactory tfactory = new TTransportFactory();
			TProtocolFactory pfactory = new TBinaryProtocol.Factory();
			this.server = new TThreadPoolServer(processor, serverTransport,
					tfactory, pfactory);
			// server = new TSimpleServer(processor, serverTransport);
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

		private Transcoder transcoder = new ByteArrayTranscoder();

		private KeyValueStore backend;

		public KeyValueStoreServiceHandler(KeyValueStore backend) {
			this.backend = backend;
		}

		public boolean exists(String key) throws KeyValueStoreIOException,
				KeyValueStoreException, TException {
			log.trace("exists()");
			try {
				return backend.exists(key);
			} catch (com.othersonline.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside exists()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside exists()", e);
				throw new KeyValueStoreIOException();
			}
		}

		public GetResult getValue(String key) throws KeyValueStoreIOException,
				KeyValueStoreException, TException {
			log.trace("getValue()");
			try {
				Object obj = backend.get(key, transcoder);
				if (obj == null)
					return new GetResult(false, new byte[] {});
				else {
					byte[] bytes = (byte[]) obj;
					return new GetResult(true, bytes);
				}
			} catch (com.othersonline.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside getValue()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside getValue()", e);
				throw new KeyValueStoreIOException();
			} catch (ClassNotFoundException e) {
				log.error("ClassNotFoundException inside getValue()", e);
				throw new KeyValueStoreException();
			}
		}

		public void setValue(String key, byte[] data)
				throws KeyValueStoreIOException, KeyValueStoreException,
				TException {
			log.trace("setValue()");
			try {
				backend.set(key, data, transcoder);
			} catch (com.othersonline.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside setValue()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside setValue()", e);
				throw new KeyValueStoreIOException();
			}
		}

		public void deleteValue(String key) throws KeyValueStoreIOException,
				KeyValueStoreException, TException {
			log.trace("deleteValue()");
			try {
				backend.delete(key);
			} catch (com.othersonline.kv.KeyValueStoreException e) {
				log.error("KeyValueStoreException inside deleteValue()", e);
				throw new KeyValueStoreException();
			} catch (IOException e) {
				log.error("IOException inside deleteValue()", e);
				throw new KeyValueStoreIOException();
			}
		}

	}
}
