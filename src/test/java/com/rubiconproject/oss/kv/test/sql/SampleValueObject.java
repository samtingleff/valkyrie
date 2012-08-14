package com.rubiconproject.oss.kv.test.sql;

public class SampleValueObject {
	private String k;

	private int x;

	private int y;

	private String s;

	public SampleValueObject(String k, int x, int y, String s) {
		this.k = k;
		this.x = x;
		this.y = y;
		this.s = s;
	}

	public String getK() {
		return k;
	}

	public int getX() {
		return x;
	}

	public int getY() {
		return y;
	}

	public String getS() {
		return s;
	}
}
