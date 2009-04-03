package com.othersonline.kv.transcoder;

import java.io.IOException;

public class IntegerTranscoder implements Transcoder {

	public Object decode(byte[] bytes) throws IOException,
			ClassNotFoundException {
		String s = new String(bytes);
		return Integer.parseInt(s);
	}

	public byte[] encode(Object value) throws IOException {
		Integer i = (Integer) value;
		return Integer.toString(i.intValue()).getBytes();
	}

}
