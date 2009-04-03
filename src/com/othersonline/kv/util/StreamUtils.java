package com.othersonline.kv.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

public class StreamUtils {
	/**
	 * Read an InputStream to bytes.
	 * 
	 * @param is
	 *            an InputStream
	 * @return byte array
	 * @throws IOException
	 * 
	 */
	public static byte[] inputStreamToBytes(InputStream is) throws IOException {
		return inputStreamToBytes(is, 256);
	}

	/**
	 * Read an InputStream to bytes using the specified buffer size.
	 * 
	 * @param is
	 *            an InputStream
	 * @param bufferSize
	 *            size of buffer
	 * @return byte array
	 * @throws IOException
	 */
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
	 *            an InputStream
	 * @return string representation of this input stream
	 * @throws IOException
	 */
	public static String inputStreamToString(InputStream is) throws IOException {
		return inputStreamToString(is, 256, "UTF-8");
	}

	/**
	 * Read an InputStream to a String using the specified buffer size
	 * 
	 * @param is
	 *            an InputStream
	 * @param bufferSize
	 *            size of buffer
	 * @return string representation of this input stream
	 * @throws IOException
	 */
	public static String inputStreamToString(InputStream is, int bufferSize)
			throws IOException {
		return inputStreamToString(is, bufferSize, "UTF-8");
	}

	/**
	 * Read an InputStream to a String using the specified buffer size and
	 * character encoding
	 * 
	 * @param is
	 *            an InputStream
	 * @param bufferSize
	 *            size of buffer
	 * @param encoding
	 *            character encoding
	 * @return string representation of this input stream
	 * @throws IOException
	 */

	public static String inputStreamToString(InputStream is, int bufferSize,
			String encoding) throws IOException {
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

	/**
	 * Copy from an InputStream to an OutputStream using the default buffer size
	 * (4 * 1024).
	 * 
	 * @param input
	 *            the InputStream
	 * @param output
	 *            the OutputStream
	 * @throws IOException
	 */
	public static void copyStreamToStream(InputStream input, OutputStream output)
			throws IOException {
		copyStreamToStream(input, output, 4 * 1024);
	}

	/**
	 * Copy from an InputStream to an OutputStream using the specified buffer
	 * size.
	 * 
	 * @param input
	 *            the InputStream
	 * @param output
	 *            the OutputStream
	 * @param bufferSize
	 *            the buffer size
	 * @throws IOException
	 */
	public static void copyStreamToStream(InputStream input,
			OutputStream output, int bufferSize) throws IOException {
		byte[] buffer = new byte[bufferSize];
		int read = 0;
		while (-1 != (read = input.read(buffer))) {
			output.write(buffer, 0, read);
		}
	}

	/**
	 * Read the specified resource into a string.
	 * 
	 * @param resource
	 *            the classpath resource
	 * @return resource as a string
	 * @throws IOException
	 */
	public static String getResourceAsString(String resource)
			throws IOException {
		InputStream is = StreamUtils.class.getResourceAsStream(resource);
		String s = inputStreamToString(is);
		is.close();
		return s;
	}
}
