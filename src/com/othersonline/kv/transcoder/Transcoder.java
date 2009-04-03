package com.othersonline.kv.transcoder;

import java.io.IOException;

public interface Transcoder {

	public byte[] encode(Object value) throws IOException;

	public Object decode(byte[] bytes) throws IOException,
			ClassNotFoundException;
}
