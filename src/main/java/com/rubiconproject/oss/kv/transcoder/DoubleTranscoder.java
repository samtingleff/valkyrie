package com.rubiconproject.oss.kv.transcoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Encoder/decoder for doubles. Copied from Tokyo Tyrant:
 * 
 * http://bitbucket.org/EP/tokyotyrant-java/src/tip/src/main/java/tokyotyrant/transcoder/DoubleTranscoder.java
 * 
 * @author samtingleff
 * 
 */
public class DoubleTranscoder implements Transcoder {
	private final ByteOrder byteOrder;

	public DoubleTranscoder() {
		this(ByteOrder.nativeOrder());
	}

	public DoubleTranscoder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}

	public byte[] encode(Object decoded) {
		return ByteBuffer.allocate(Double.SIZE / 8).order(byteOrder).putDouble(
				(Double) decoded).array();
	}

	public Object decode(byte[] encoded) {
		if (encoded.length != Double.SIZE / 8) {
			throw new IllegalArgumentException("Unable to decode "
					+ Arrays.toString(encoded));
		}
		return ByteBuffer.wrap(encoded).order(byteOrder).getDouble();
	}

}
