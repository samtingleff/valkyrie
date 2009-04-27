package com.othersonline.kv.transcoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzippingTranscoder implements Transcoder {
	private Transcoder delegate;

	public GzippingTranscoder() {
		this.delegate = new SerializableTranscoder();
	}

	public GzippingTranscoder(Transcoder delegate) {
		this.delegate = delegate;
	}

	public Object decode(byte[] bytes) throws IOException,
			ClassNotFoundException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		GZIPInputStream gunzip = new GZIPInputStream(in);
		byte[] buffer = new byte[bytes.length];
		int read = 0;
		while ((read = gunzip.read(buffer)) > 0) {
			baos.write(buffer, 0, read);
		}
		gunzip.close();
		in.close();
		baos.close();
		byte[] uncompressed = baos.toByteArray();
		Object obj = delegate.decode(uncompressed);
		return obj;
	}

	public byte[] encode(Object value) throws IOException {
		byte[] uncompressed = delegate.encode(value);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(uncompressed);
		gzip.close();
		out.close();
		return out.toByteArray();
	}

}
