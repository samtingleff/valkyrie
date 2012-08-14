package com.othersonline.kv.transcoder;

import java.util.Arrays;

/**
 * Encoder/decoder for bytes. Copied from Tokyo Tyrant:
 * 
 * http://bitbucket.org/EP/tokyotyrant-java/src/tip/src/main/java/tokyotyrant/transcoder/ByteTranscoder.java
 * 
 * @author samtingleff
 * 
 */
public class ByteTranscoder implements Transcoder {
	public byte[] encode(Object decoded) {
		return new byte[] { (Byte) decoded };
	}

	public Object decode(byte[] encoded) {
		if (encoded.length != Byte.SIZE / 8) {
			throw new IllegalArgumentException("Unable to decode "
					+ Arrays.toString(encoded));
		}
		return encoded[0];
	}

}
