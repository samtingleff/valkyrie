package com.othersonline.kv.distributed.hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/**
 * Borrowed from the spy memcached client, which has the following license.
 * 
 * Copyright (c) 2006-2009 Dustin Sallings <dustin@spy.net>
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 * 
 * @author Dustin Sallings <dustin@spy.net>
 * 
 */
public class KetamaHashAlgorithm implements HashAlgorithm {

	public long hash(final String key) {
		byte[] bKey = md5(key);
		long l = ((long) (bKey[3] & 0xFF) << 24)
				| ((long) (bKey[2] & 0xFF) << 16)
				| ((long) (bKey[1] & 0xFF) << 8) | (bKey[0] & 0xFF);
		return l;
	}

	public byte[] md5(final String key) {
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			md5.reset();
			md5.update(key.getBytes());
			byte[] bytes = md5.digest();
			return bytes;
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("Cannot find MD5 message digest!");
		}

	}
}
