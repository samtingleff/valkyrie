package com.othersonline.kv.backends;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.util.StreamUtils;

public class WebDAVKeyValueStore extends BaseManagedKeyValueStore implements
		KeyValueStore {

	private static final String IDENTIFIER = "webdav";

	private Log log = LogFactory.getLog(getClass());

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private HttpClient httpClient;

	private String baseUrl;

	private int maxConnectionsPerHost = 500;

	private int maxTotalConnections = 500;

	private int socketTimeout = 500;

	private int connectionTimeout = 500;

	private int responseBufferSize = 1024;

	public WebDAVKeyValueStore() {
	}

	public WebDAVKeyValueStore(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	public void setBaseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
		this.maxConnectionsPerHost = maxConnectionsPerHost;
	}

	public void setMaxTotalConnections(int maxTotalConnections) {
		this.maxTotalConnections = maxTotalConnections;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public void setResponseBufferSize(int responseBufferSize) {
		this.responseBufferSize = responseBufferSize;
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void start() throws IOException {
		MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
		mgr.getParams().setDefaultMaxConnectionsPerHost(maxConnectionsPerHost);
		mgr.getParams().setMaxTotalConnections(maxTotalConnections);
		mgr.getParams().setSoTimeout(socketTimeout);
		mgr.getParams().setConnectionTimeout(connectionTimeout);
		httpClient = new HttpClient(mgr);
		super.start();
	}

	@Override
	public void stop() {
		httpClient = null;
		super.stop();
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		HeadMethod method = new HeadMethod(getUri(key));
		try {
			HttpResponse response = executeMethod(method);
			if (response.responseCode == 200) {
				return true;
			} else {
				return false;
			}
		} catch (HttpException e) {
			log.error("HttpException inside exists()", e);
			throw new IOException(e);
		} catch (IOException e) {
			log.error("IOException inside exists()", e);
			throw new IOException(e);
		} finally {
			try {
				method.releaseConnection();
			} catch (Exception e) {
			}
		}
	}

	@Override
	public Object get(String key) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		return get(key, defaultTranscoder);
	}

	@Override
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
		assertReadable();
		GetMethod method = new GetMethod(getUri(key));
		try {
			HttpResponse response = executeMethod(method);
			if (response.responseCode == 200) {
				InputStream is = response.responseBody;
				byte[] bytes = StreamUtils.inputStreamToBytes(is, is
						.available());
				is.close();
				Object obj = transcoder.decode(bytes);
				return obj;
			} else {
				return null;
			}
		} catch (EOFException e) {
			return null;
		} catch (HttpException e) {
			log.error("HttpException inside get()", e);
			throw new IOException(e);
		} catch (IOException e) {
			log.error("IOException inside get()", e);
			throw new IOException(e);
		} finally {
			try {
				method.releaseConnection();
			} catch (Exception e) {
			}
		}
	}

	@Override
	public void set(String key, Serializable value)
			throws KeyValueStoreException, IOException {
		set(key, value, defaultTranscoder);
	}

	@Override
	public void set(String key, Serializable value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		String uri = getUri(key);
		PutMethod method = new PutMethod(uri);
		try {
			byte[] bytes = transcoder.encode(value);
			RequestEntity body = new ByteArrayRequestEntity(bytes);
			method.setRequestEntity(body);
			HttpResponse response = executeMethod(method);
			if (response.responseCode == 403) {
				// sent if the parent "folder" does not exist
				// given a uri like /blobs/user/12/100/177.json
				// need to call mkcol: /blobs, /blobs/user, /blobs/user/12, etc.
				method.releaseConnection();
				String collectionUri = uri.substring(0, uri.lastIndexOf('/'));
				HttpResponse mkcolResponse = mkcol(collectionUri);
				List<String> mkdirs = new LinkedList<String>();
				while ((mkcolResponse.responseCode == 403)
						|| (mkcolResponse.responseCode == 409)) {
					mkdirs.add(0, collectionUri);
					collectionUri = collectionUri.substring(0, collectionUri
							.lastIndexOf('/'));
					mkcolResponse = mkcol(collectionUri);
				}
				for (String string : mkdirs) {
					mkcolResponse = mkcol(string);
				}
				response = executeMethod(method);
			}
			if ((response.responseCode < 200) && (response.responseCode >= 300)) {
				throw new KeyValueStoreException("Got non-okay http response "
						+ response.responseCode);
			}
		} catch (UnsupportedEncodingException e) {
			log.error("UnsupportedEncodingException inside put()", e);
			throw new KeyValueStoreException(e);
		} catch (HttpException e) {
			log.error("HttpException inside put()", e);
			throw new IOException(e);
		} catch (IOException e) {
			log.error("IOException inside put()", e);
			throw new IOException(e);
		} finally {
			try {
				method.releaseConnection();
			} catch (Exception e) {
			}
		}
	}

	@Override
	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		DeleteMethod method = new DeleteMethod(getUri(key));
		try {
			HttpResponse response = executeMethod(method);
			if (((response.responseCode < 200) || (response.responseCode >= 300))
					&& (response.responseCode != 404)) {
				throw new KeyValueStoreException("Got non-okay http response "
						+ response.responseCode);
			}
		} catch (HttpException e) {
			log.error("HttpException inside delete()", e);
			throw new IOException(e);
		} catch (IOException e) {
			log.error("IOException inside delete()", e);
			throw new IOException(e);
		} finally {
			try {
				method.releaseConnection();
			} catch (Exception e) {
			}
		}
	}

	private String getUri(String key) {
		return baseUrl + key;
	}

	private HttpResponse mkcol(String uri) throws HttpException, IOException {
		HttpMkcolMethod method = new HttpMkcolMethod(uri);
		try {
			int responseCode = httpClient.executeMethod(method);
			InputStream is = method.getResponseBodyAsStream();
			return new HttpResponse(responseCode, is);
		} finally {
			method.releaseConnection();
		}
	}

	private HttpResponse executeMethod(HttpMethod method) throws HttpException,
			IOException {
		int responseCode = httpClient.executeMethod(method);
		InputStream responseBody = method.getResponseBodyAsStream();
		return new HttpResponse(responseCode, responseBody);
	}

	private static class HttpMkcolMethod extends HttpMethodBase implements
			HttpMethod {
		public HttpMkcolMethod(String uri) {
			super(uri);
		}

		@Override
		public String getName() {
			return "MKCOL";
		}

	}

	private static class HttpResponse {
		int responseCode;

		InputStream responseBody;

		public HttpResponse(int responseCode, InputStream responseBody) {
			this.responseCode = responseCode;
			this.responseBody = responseBody;
		}
	}
}
