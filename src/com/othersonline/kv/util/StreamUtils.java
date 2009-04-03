package com.othersonline.kv.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamUtils {
	/**
	 * Read an InputStream to bytes.
	 * 
	 * @throws IOException
	 * 
	 */
	public static byte[] inputStreamToBytes(InputStream is) throws IOException {
		return inputStreamToBytes(is, 256);
	}

	public static byte[] inputStreamToBytes(InputStream is, int bufferSize)
			throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buf = new byte[bufferSize];
		int pos = 0;
		int len = 64;
		int count = 0;
		while ((count = is.read(buf, pos, len)) > 0) {
			baos.write(buf, 0, count);
		}
		return baos.toByteArray();
	}

	/**
	 * Read an InputStream to a String.
	 * 
	 * @param is
	 * @return string representation of this input stream
	 * @throws IOException
	 */
	public static String inputStreamToString(InputStream is) throws IOException {
		return inputStreamToString(is, "UTF-8", 256);
	}

	public static String inputStreamToString(InputStream is, int bufferSize)
			throws IOException {
		return inputStreamToString(is, "UTF-8", bufferSize);
	}

	public static String inputStreamToString(InputStream is, String encoding,
			int bufferSize) throws IOException {
		StringBuffer buffer = new StringBuffer(bufferSize);
		// UTF-8, US-ASCII, UTF-16
		InputStreamReader reader = new InputStreamReader(is, encoding);
		char[] buf = new char[bufferSize];
		int pos = 0;
		int len = 64;
		int count = 0;
		while ((count = reader.read(buf, pos, len)) > 0) {
			buffer.append(buf, 0, count);
		}

		return buffer.toString();
	}
}
