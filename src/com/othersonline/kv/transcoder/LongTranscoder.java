package com.othersonline.kv.transcoder;

import java.io.IOException;

public class LongTranscoder implements Transcoder {

	public Object decode(byte[] bytes) throws IOException,
			ClassNotFoundException {
		String s = new String(bytes);
		return Long.parseLong(s);
	}

	public byte[] encode(Object value) throws IOException {
		Long i = (Long) value;
		return Long.toString(i.longValue()).getBytes();
	}

}
