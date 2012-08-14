package com.othersonline.kv.transcoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializableTranscoder implements Transcoder {

	public Object decode(byte[] bytes) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		ObjectInputStream ois = new ObjectInputStream(bais);
		try {
			Object obj = ois.readObject();
			ois.close();
			bais.close();
			return obj;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] encode(Object value) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(value);
		oos.close();
		baos.close();
		byte[] bytes = baos.toByteArray();
		return bytes;
	}

}
