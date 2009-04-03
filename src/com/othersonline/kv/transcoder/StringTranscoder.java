package com.othersonline.kv.transcoder;

import java.io.IOException;

public class StringTranscoder implements Transcoder {

	public Object decode(byte[] bytes) throws IOException,
			ClassNotFoundException {
		return new String(bytes);
	}

	public byte[] encode(Object value) throws IOException {
		return ((String) value).getBytes();
	}

}
