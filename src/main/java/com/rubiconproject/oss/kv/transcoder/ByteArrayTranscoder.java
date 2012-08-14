package com.rubiconproject.oss.kv.transcoder;

/**
 * Pass-through encoder/decoder for byte arrays. Copied from Tokyo Tyrant:
 * 
 * http://bitbucket.org/EP/tokyotyrant-java/src/tip/src/main/java/tokyotyrant/transcoder/ByteArrayTranscoder.java
 * 
 * @author samtingleff
 * 
 */
public class ByteArrayTranscoder implements Transcoder {
	public byte[] encode(Object decoded) {
		if (decoded == null) {
			throw new NullPointerException("Cannot encode null");
		}
		return (byte[]) decoded;
	}

	public Object decode(byte[] encoded) {
		if (encoded == null) {
			throw new NullPointerException("Cannot decode null");
		}
		return encoded;
	}
}
