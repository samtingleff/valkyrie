package com.rubiconproject.oss.kv.distributed.impl;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
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

	/*
	 * Attempt at an iterator that will not throw
	 * ConcurrentModificationException when new results are appended to the
	 * list.
	 */
	public Iterator<V> iterator() {
		return new Iterator<V>() {
			private int index = 0;

			private List<V> delegate;

			public Iterator<V> init(List<V> delegate) {
				this.delegate = delegate;
				return this;
			}

			public boolean hasNext() {
				return (index < delegate.size());
			}

			public V next() {
				V next = null;
				try {
					next = delegate.get(index);
				} catch (IndexOutOfBoundsException e) {
					throw new ConcurrentModificationException(e.getMessage());
				} finally {
					++index;
				}
				return next;
			}

			public void remove() {
			}
		}.init(this.results);
	}
}
