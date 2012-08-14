package com.othersonline.kv.transcoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZippingTranscoder implements Transcoder {
	private static final String ZIP_ENTRY_NAME = "entry";

	private Transcoder delegate;

	public ZippingTranscoder() {
		this.delegate = new SerializableTranscoder();
	}

	public ZippingTranscoder(Transcoder delegate) {
		this.delegate = delegate;
	}

	public Object decode(byte[] bytes) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		ZipInputStream unzip = new ZipInputStream(in);
		ZipEntry entry = unzip.getNextEntry();
		byte[] buffer = new byte[bytes.length];
		int read = 0;
		while ((read = unzip.read(buffer)) > 0) {
			baos.write(buffer, 0, read);
		}
		unzip.closeEntry();
		unzip.close();
		in.close();
		baos.close();
		byte[] uncompressed = baos.toByteArray();
		Object obj = delegate.decode(uncompressed);
		return obj;
	}

	public byte[] encode(Object value) throws IOException {
		byte[] uncompressed = delegate.encode(value);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ZipOutputStream zip = new ZipOutputStream(out);
		ZipEntry entry = new ZipEntry(ZIP_ENTRY_NAME);
		zip.putNextEntry(entry);
		zip.write(uncompressed);
		zip.closeEntry();
		zip.close();
		out.close();
		return out.toByteArray();
	}

}
