package com.rubiconproject.oss.kv.tuple;

/**
 * Ordered set of elements.
 * <p>
 * You can use a tuple as a multi-valued key for hash-based or
 * ordered collections. Tuples properly implement hashCode and
 * equals to compare all items in the tuple with respect to order.
 * As a key to an ordered collection, the tuple gives the first
 * element precedence.
 * <p>
 * You can also use a tuple to return multiple values from a function.
 * Use one of the shorthand types ({@link Tuple2}, {@link Tuple3}, etc.)
 * as the return type for the function. Use one of the overloaded
 * {@link Tuple#from(Object,Object)} methods to
 * generate the tuple within the function. Use the {@link Tuple#extract(Variable)} method
 * to fetch values from the tuple back in the caller.
 *
 * @author Michael L Perry
 *
 * @param <First> The type of the first element in the tuple.
 * @param <Rest> The type of the tuple containing the rest of the elements.
 * 
 */
/*
 * Copyright (c) 2008, Mallard Software Designs, Inc.
 * http://mallardsoft.com
 * All rights reserved.
 * 
 * Redistribution and use of this software in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     * Redistributions of source code must retain the above copyright notice, this list
 *       of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright notice, this
 *       list of conditions and the following disclaimer in the documentation and/or other
 *       materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
public class Tuple<First extends Comparable<First>, Rest extends Comparable<Rest>> implements SeparatedAppender, Comparable<Tuple<First, Rest>> {

	protected First first;
	protected Rest rest;

	protected Tuple(First first, Rest rest) {
		this.first = first;
		this.rest = rest;
	}

	/**
	 * Remove the first element from the tuple and return the rest.
	 * To extract all elements from the tuple, chain extract calls.
	 *
	 * @param m
	 * @return
	 */
	public Rest extract(Variable<First> m) {
		m.set(first);
		return rest;
	}

	public <T extends Comparable<T>> Tuple<T, Tuple<First, Rest>> prepend(T m) {
		return new Tuple<T, Tuple<First, Rest>>(m, this);
	}

    // Compare two tuples. All elements must be equal.
    @SuppressWarnings("unchecked")
	public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (!(obj instanceof Tuple))
            return false;
        Tuple<First, Rest> that = (Tuple<First, Rest>) obj;
        return
        	(this.first == null ? that.first == null : this.first.equals(that.first)) &&
        	this.rest.equals(that.rest);
    }

    // Calculate a hash code based on the hash of each element.
    public int hashCode() {
    	return (first == null ? 0 : first.hashCode()) + rest.hashCode() * 37;
    }

    public String toString() {
    	return toString("(", ", ", ")");
    }

    // Display the tuple using the open, separator, and close.
    public String toString(String open, String separator, String close) {
        StringBuffer result = new StringBuffer();
        result.append(open).append(first);
    	((SeparatedAppender)rest).appendString(result, separator);
        return result.append(close).toString();
    }

    public void appendString(StringBuffer buffer, String separator) {
    	buffer.append(separator).append(first);
    	((SeparatedAppender)rest).appendString(buffer, separator);
	}

	// Order by the most significant element first.
    // The tuples must agree in size and type.
	public int compareTo(Tuple<First, Rest> that) {
        int compare = this.first.compareTo(that.first);
        if (compare != 0)
        	return compare;
        else
        	return this.rest.compareTo(that.rest);
    }

    public static <T1 extends Comparable<T1>> Tuple1<T1> from(T1 m1) {
    	return new Tuple1<T1>(m1);
    }

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>> Tuple2<T1, T2> from(T1 m1, T2 m2) {
		return new Tuple2<T1, T2>(m1, m2);
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>> Tuple3<T1, T2, T3> from(T1 m1, T2 m2, T3 m3) {
		return new Tuple3<T1, T2, T3>(m1, m2, m3);
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>> Tuple4<T1, T2, T3, T4> from(T1 m1, T2 m2, T3 m3, T4 m4) {
		return new Tuple4<T1, T2, T3, T4>(m1, m2, m3, m4);
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>> Tuple5<T1, T2, T3, T4, T5> from(T1 m1, T2 m2, T3 m3, T4 m4, T5 m5) {
		return new Tuple5<T1, T2, T3, T4, T5>(m1, m2, m3, m4, m5);
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>> Tuple6<T1, T2, T3, T4, T5, T6> from(T1 m1, T2 m2, T3 m3, T4 m4, T5 m5, T6 m6) {
		return new Tuple6<T1, T2, T3, T4, T5, T6>(m1, m2, m3, m4, m5, m6);
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>, T7 extends Comparable<T7>> Tuple7<T1, T2, T3, T4, T5, T6, T7> from(T1 m1, T2 m2, T3 m3, T4 m4, T5 m5, T6 m6, T7 m7) {
		return new Tuple7<T1, T2, T3, T4, T5, T6, T7>(m1, m2, m3, m4, m5, m6, m7);
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>, T7 extends Comparable<T7>, T8 extends Comparable<T8>> Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> from(T1 m1, T2 m2, T3 m3, T4 m4, T5 m5, T6 m6, T7 m7, T8 m8) {
		return new Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>(m1, m2, m3, m4, m5, m6, m7, m8);
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>, T7 extends Comparable<T7>, T8 extends Comparable<T8>, T9 extends Comparable<T9>> Tuple9<T1, T2, T3, T4, T5, T6, T7, T8, T9> from(T1 m1, T2 m2, T3 m3, T4 m4, T5 m5, T6 m6, T7 m7, T8 m8, T9 m9) {
		return new Tuple9<T1, T2, T3, T4, T5, T6, T7, T8, T9>(m1, m2, m3, m4, m5, m6, m7, m8, m9);
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>, T7 extends Comparable<T7>, T8 extends Comparable<T8>, T9 extends Comparable<T9>, T10 extends Comparable<T10>> Tuple10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> from(T1 m1, T2 m2, T3 m3, T4 m4, T5 m5, T6 m6, T7 m7, T8 m8, T9 m9, T10 m10) {
		return new Tuple10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10);
	}

	public static <T1 extends Comparable<T1>, Rest extends Comparable<Rest>> T1 get1(Tuple<T1, Rest> tuple) {
		return tuple.first;
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, Rest extends Comparable<Rest>> T2 get2(Tuple<T1, Tuple<T2, Rest>> tuple) {
		return tuple.rest.first;
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, Rest extends Comparable<Rest>> T3 get3(Tuple<T1, Tuple<T2, Tuple<T3, Rest>>> tuple) {
		return tuple.rest.rest.first;
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, Rest extends Comparable<Rest>> T4 get4(Tuple<T1, Tuple<T2, Tuple<T3, Tuple<T4, Rest>>>> tuple) {
		return tuple.rest.rest.rest.first;
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, Rest extends Comparable<Rest>> T5 get5(Tuple<T1, Tuple<T2, Tuple<T3, Tuple<T4, Tuple<T5, Rest>>>>> tuple) {
		return tuple.rest.rest.rest.rest.first;
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>, Rest extends Comparable<Rest>> T6 get6(Tuple<T1, Tuple<T2, Tuple<T3, Tuple<T4, Tuple<T5, Tuple<T6, Rest>>>>>> tuple) {
		return tuple.rest.rest.rest.rest.rest.first;
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>, T7 extends Comparable<T7>, Rest extends Comparable<Rest>> T7 get7(Tuple<T1, Tuple<T2, Tuple<T3, Tuple<T4, Tuple<T5, Tuple<T6, Tuple<T7, Rest>>>>>>> tuple) {
		return tuple.rest.rest.rest.rest.rest.rest.first;
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>, T7 extends Comparable<T7>, T8 extends Comparable<T8>, Rest extends Comparable<Rest>> T8 get8(Tuple<T1, Tuple<T2, Tuple<T3, Tuple<T4, Tuple<T5, Tuple<T6, Tuple<T7, Tuple<T8, Rest>>>>>>>> tuple) {
		return tuple.rest.rest.rest.rest.rest.rest.rest.first;
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>, T7 extends Comparable<T7>, T8 extends Comparable<T8>, T9 extends Comparable<T9>, Rest extends Comparable<Rest>> T9 get9(Tuple<T1, Tuple<T2, Tuple<T3, Tuple<T4, Tuple<T5, Tuple<T6, Tuple<T7, Tuple<T8, Tuple<T9, Rest>>>>>>>>> tuple) {
		return tuple.rest.rest.rest.rest.rest.rest.rest.rest.first;
	}

	public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>, T4 extends Comparable<T4>, T5 extends Comparable<T5>, T6 extends Comparable<T6>, T7 extends Comparable<T7>, T8 extends Comparable<T8>, T9 extends Comparable<T9>, T10 extends Comparable<T10>, Rest extends Comparable<Rest>> T10 get10(Tuple<T1, Tuple<T2, Tuple<T3, Tuple<T4, Tuple<T5, Tuple<T6, Tuple<T7, Tuple<T8, Tuple<T9, Tuple<T10, Rest>>>>>>>>>> tuple) {
		return tuple.rest.rest.rest.rest.rest.rest.rest.rest.rest.first;
	}

}
