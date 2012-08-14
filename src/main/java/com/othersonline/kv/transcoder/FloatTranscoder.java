package com.othersonline.kv.transcoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Encoder/decoder for float objects. Copied from Tokyo Tyrant:
 * 
 * http://bitbucket.org/EP/tokyotyrant-java/src/tip/src/main/java/tokyotyrant/transcoder/FloatTranscoder.java
 * 
 * @author samtingleff
 * 
 */
public class FloatTranscoder implements Transcoder {
	private final ByteOrder byteOrder;

	public FloatTranscoder() {
		this(ByteOrder.nativeOrder());
	}

	public FloatTranscoder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}

	public byte[] encode(Object decoded) {
		return ByteBuffer.allocate(Float.SIZE / 8).order(byteOrder).putFloat(
				(Float) decoded).array();
	}

	public Float decode(byte[] encoded) {
		if (encoded.length != Float.SIZE / 8) {
			throw new IllegalArgumentException("Unable to decode "
					+ Arrays.toString(encoded));
		}
		return ByteBuffer.wrap(encoded).order(byteOrder).getFloat();
	}

}
