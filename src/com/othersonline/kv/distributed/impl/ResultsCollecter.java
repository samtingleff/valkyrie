package com.othersonline.kv.distributed.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ResultsCollecter<V> implements Iterable<V> {
	private List<V> results;
	private boolean stopped = false;
	public ResultsCollecter(int size) {
		results = new ArrayList<V>(size);
	}
	public void add(V result) {
		if (!stopped)
		results.add(result);
	}
	public int size() {
		return results.size();
	}
	public void stop() {
		this.stopped = true;
	}
	public Iterator<V> iterator() {
		return results.iterator();
	}
}
