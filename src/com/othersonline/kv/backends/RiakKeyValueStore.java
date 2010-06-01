package com.othersonline.kv.backends;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.StoreResponse;
import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class RiakKeyValueStore extends BaseManagedKeyValueStore implements
		KeyValueStore {

	private static final String IDENTIFIER = "riak";

	private Log log = LogFactory.getLog(getClass());

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private RequestMeta writeRequestMeta;

	private RequestMeta readRequestMeta;

	private RiakClient client;

	private HttpClient httpClient;

	private String host;

	private int port = 8098;

	private String prefix = "/riak";

	private String baseUrl;

	private String bucket;

	private int w = 1;

	private int dw = 0;

	private int r = 1;

	private int maxConnectionsPerHost = 500;

	private int maxTotalConnections = 500;

	private int socketTimeout = 5000;

	private int connectionTimeout = 5000;

	public RiakKeyValueStore() {
	}

	public RiakKeyValueStore(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	@Configurable(name = "host", accepts = Type.StringType)
	public void setHost(String host) {
		this.host = host;
	}

	@Configurable(name = "port", accepts = Type.IntType)
	public void setPort(int port) {
		this.port = port;
	}

	@Configurable(name = "prefix", accepts = Type.StringType)
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	@Configurable(name = "baseUrl", accepts = Type.StringType)
	public void setBaseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	@Configurable(name = "bucket", accepts = Type.StringType)
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	@Configurable(name = "writePolicy", accepts = Type.IntType)
	public void setWritePolicy(int w) {
		this.w = w;
	}

	@Configurable(name = "durableWritePolicy", accepts = Type.IntType)
	public void setDurableWritePolicy(int dw) {
		this.dw = dw;
	}

	@Configurable(name = "readPolicy", accepts = Type.IntType)
	public void setReadPolicy(int r) {
		this.r = r;
	}

	@Configurable(name = "maxConnectionsPerHost", accepts = Type.IntType)
	public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
		this.maxConnectionsPerHost = maxConnectionsPerHost;
	}

	@Configurable(name = "maxConnectionsPerHost", accepts = Type.IntType)
	public void setMaxTotalConnections(int maxTotalConnections) {
		this.maxTotalConnections = maxTotalConnections;
	}

	@Configurable(name = "maxConnectionsPerHost", accepts = Type.IntType)
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	@Configurable(name = "connectionTimeout", accepts = Type.IntType)
	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
		mgr.getParams().setDefaultMaxConnectionsPerHost(maxConnectionsPerHost);
		mgr.getParams().setMaxTotalConnections(maxTotalConnections);
		mgr.getParams().setSoTimeout(socketTimeout);
		mgr.getParams().setConnectionTimeout(connectionTimeout);
		httpClient = new HttpClient(mgr);

		if (baseUrl == null)
			baseUrl = String.format("http://%1$s:%2$d%3$s", host, port, prefix);

		RiakConfig config = new RiakConfig(baseUrl);
		config.setHttpClient(httpClient);
		client = new RiakClient(config);

		writeRequestMeta = new RequestMeta().writeParams(w, dw).setQueryParam(
				"returnbody", "false");
		readRequestMeta = new RequestMeta().readParams(r);

		super.start();
	}

	public void stop() {
		httpClient = null;
		super.stop();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		FetchResponse r = client.fetchMeta(bucket, getKey(key), RequestMeta
				.readParams(2));
		return r.isSuccess();
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Object result = null;
		FetchResponse r = client.fetch(bucket, getKey(key), readRequestMeta);
		try {
			if (r.isSuccess()) {
				byte[] bytes = r.getBody();
				result = transcoder.decode(bytes);
			}
		} finally {
			r.close();
		}
		return result;
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		List<String> coll = Arrays.asList(keys);
		return getBulk(coll);
	}

	public Map<String, Object> getBulk(final List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null) {
				results.put(key, obj);
			}
		}
		return results;
	}

	public Map<String, Object> getBulk(final List<String> keys,
			Transcoder transcoder) throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key, transcoder);
			if (obj != null) {
				results.put(key, obj);
			}
		}
		return results;
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		RiakObject o = new RiakObject(bucket, getKey(key));
		byte[] bytes = transcoder.encode(value);
		o.setValue(bytes);
		StoreResponse r = client.store(o, writeRequestMeta);
		if (r.isSuccess())
			o.updateMeta(r);
		else
			throw new KeyValueStoreException("Response code: "
					+ r.getStatusCode());
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		HttpResponse response = client.delete(bucket, getKey(key));
		if (((response.getStatusCode() < 200) || (response.getStatusCode() >= 300))
				&& (response.getStatusCode() != 404)) {
			throw new KeyValueStoreException("Got non-okay http response "
					+ response.getStatusCode());
		}
	}

	private String getKey(String rawKey) {
		try {
			return URLEncoder.encode(rawKey, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
}
