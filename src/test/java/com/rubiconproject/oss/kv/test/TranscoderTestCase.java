package com.rubiconproject.oss.kv.test;

import java.io.Serializable;
import java.util.Date;

import com.rubiconproject.oss.kv.transcoder.ByteArrayTranscoder;
import com.rubiconproject.oss.kv.transcoder.ByteTranscoder;
import com.rubiconproject.oss.kv.transcoder.DoubleTranscoder;
import com.rubiconproject.oss.kv.transcoder.FloatTranscoder;
import com.rubiconproject.oss.kv.transcoder.GzippingTranscoder;
import com.rubiconproject.oss.kv.transcoder.IntegerTranscoder;
import com.rubiconproject.oss.kv.transcoder.LongTranscoder;
import com.rubiconproject.oss.kv.transcoder.SerializableTranscoder;
import com.rubiconproject.oss.kv.transcoder.SerializingTranscoder;
import com.rubiconproject.oss.kv.transcoder.StringTranscoder;
import com.rubiconproject.oss.kv.transcoder.Transcoder;
import com.rubiconproject.oss.kv.transcoder.ZippingTranscoder;
import com.rubiconproject.oss.kv.util.StreamUtils;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

public class TranscoderTestCase extends TestCase {

	public void testByteArrayTranscoder() throws Exception {
		byte[] bytes = "hello world".getBytes();
		Transcoder t = new ByteArrayTranscoder();
		assertEquals(t.encode(bytes), bytes);
		assertEquals(t.decode(t.encode(bytes)), bytes);
	}

	public void testByteTranscoder() throws Exception {
		byte b = 'a';
		Transcoder t = new ByteTranscoder();
		assertEquals(t.decode(t.encode(b)), b);
	}

	public void testDoubleTranscoder() throws Exception {
		double d = 10.24343234;
		Transcoder t = new DoubleTranscoder();
		assertEquals(t.decode(t.encode(d)), d);
	}

	public void testFloatTranscoder() throws Exception {
		float f = 12312312.123123f;
		Transcoder t = new FloatTranscoder();
		assertEquals(t.decode(t.encode(f)), f);
	}

	public void testIntegerTranscoder() throws Exception {
		int i = 12312312;
		Transcoder t = new IntegerTranscoder();
		assertEquals(t.decode(t.encode(i)), i);
	}

	public void testLongTranscoder() throws Exception {
		long l = 9824783295l;
		Transcoder t = new LongTranscoder();
		assertEquals(t.decode(t.encode(l)), l);
	}

	public void testStringTranscoder() throws Exception {
		String s = "This is a string here.";
		Transcoder t = new StringTranscoder();
		assertEquals(t.decode(t.encode(s)), s);
	}

	public void testSerializableTranscoder() throws Exception {
		SomeSerializableObject obj = new SomeSerializableObject("hey man",
				123123);
		Transcoder t = new SerializableTranscoder();
		assertEquals(t.decode(t.encode(obj)), obj);
	}

	public void testSerializingTranscoder() throws Exception {
		Transcoder t = new SerializingTranscoder();
		Object[] obj = new Object[] { "hello world", Boolean.TRUE, 123,
				987654321l, new Date(), 'b', 734234.23423f, 234324.2343d,
				new SomeSerializableObject("test string", 23423) };
		for (Object object : obj) {
			assertEquals(t.decode(t.encode(object)), object);
		}
		// above code fails on byte arrays - array equality is pointer-based
		String s1 = "Hello world";
		byte[] bytes = s1.getBytes();
		String s2 = new String((byte[]) t.decode(t.encode(bytes)));
		assertEquals(s1, s2);
	}

	public void testGzippingTranscoder() throws Exception {
		// try with a string
		Transcoder delegate = new StringTranscoder();
		Transcoder t = new GzippingTranscoder(delegate);
		String s = StreamUtils
				.getResourceAsString("/com/othersonline/kv/test/resources/lorem-ipsum.txt");
		byte[] raw = delegate.encode(s);
		byte[] compressed = t.encode(s);
		// length of compressed byte should be at least 1/3 the raw bytes
		assertTrue(raw.length > compressed.length);
		assertTrue((raw.length / 3) > compressed.length);
		String recovered = (String) t.decode(compressed);
		assertEquals(s, recovered);

		// try with an object
		t = new GzippingTranscoder();
		SomeSerializableObject obj = new SomeSerializableObject("hey man",
				123123);
		assertEquals(t.decode(t.encode(obj)), obj);
	}

	public void testZippingTranscoder() throws Exception {
		// try with a string
		Transcoder delegate = new StringTranscoder();
		Transcoder t = new ZippingTranscoder(delegate);
		String s = StreamUtils
				.getResourceAsString("/com/othersonline/kv/test/resources/lorem-ipsum.txt");
		byte[] raw = delegate.encode(s);
		byte[] compressed = t.encode(s);
		// length of compressed byte should be at least 1/3 the raw bytes
		assertTrue(raw.length > compressed.length);
		assertTrue((raw.length / 3) > compressed.length);
		String recovered = (String) t.decode(compressed);
		assertEquals(s, recovered);

		// try with an object
		t = new ZippingTranscoder();
		SomeSerializableObject obj = new SomeSerializableObject("hey man",
				123123);
		assertEquals(t.decode(t.encode(obj)), obj);
	}

	private static class SomeSerializableObject implements Serializable {
		private static final long serialVersionUID = -4965005264639234107L;

		private String someString;

		private int someInt;

		public SomeSerializableObject() {
		}

		public SomeSerializableObject(String someString, int someInt) {
			this.someString = someString;
			this.someInt = someInt;
		}

		public boolean equals(Object obj) {
			SomeSerializableObject s = (SomeSerializableObject) obj;
			return ((s.someString.equals(someString)) && (s.someInt == someInt));
		}
	}
}
