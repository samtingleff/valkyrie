package com.othersonline.kv.transcoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Encode/decode strings to/from byte arrays using the specified encoding.
 * Copied from the Tokyo Tyrant StringTranscoder:
 * 
 * http://bitbucket.org/EP/tokyotyrant-java/src/tip/src/main/java/tokyotyrant/transcoder/StringTranscoder.java
 * 
 * @author samtingleff
 * 
 */
public class StringTranscoder implements Transcoder {

	private String encoding;

	public StringTranscoder() {
		this("UTF-8");
	}

	public StringTranscoder(String encoding) {
		this.encoding = encoding;
	}

	public Object decode(byte[] bytes) throws IOException {
		try {
			return new String(bytes, encoding);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(
					String
							.format(
									"Unable to decode byte array to string using encoding %1$s",
									encoding), e);
		}

	}

	public byte[] encode(Object value) throws IOException {
		try {
			return ((String) value).getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(
					String
							.format(
									"Unable to encode byte array from object using encoding %1$s",
									encoding), e);
		}
	}

}
