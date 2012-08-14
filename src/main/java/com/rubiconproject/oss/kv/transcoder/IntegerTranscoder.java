package com.rubiconproject.oss.kv.transcoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Encoder/decoder for integer objects. Copied from Tokyo Tyrant:
 * 
 * http://bitbucket.org/EP/tokyotyrant-java/src/tip/src/main/java/tokyotyrant/transcoder/IntegerTranscoder.java
 * 
 * @author samtingleff
 * 
 */
public class IntegerTranscoder implements Transcoder {
	private final ByteOrder byteOrder;

	public IntegerTranscoder() {
		this(ByteOrder.nativeOrder());
	}

	public IntegerTranscoder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}

	public byte[] encode(Object decoded) {
		return ByteBuffer.allocate(Integer.SIZE / 8).order(byteOrder).putInt(
				(Integer) decoded).array();
	}

	public Object decode(byte[] encoded) {
		if (encoded.length != Integer.SIZE / 8) {
			throw new IllegalArgumentException("Unable to decode "
					+ Arrays.toString(encoded));
		}
		return ByteBuffer.wrap(encoded).order(byteOrder).getInt();
	}

}
