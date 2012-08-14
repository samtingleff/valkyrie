package com.rubiconproject.oss.kv.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * A method-level annotation that defines a particular method as "configurable".
 * Currently used in the com.oo.kv.distributed package to allow query-parameter
 * type settings on key value store connection strings.
 * 
 * A method tagged with this annotation should return no value and accept one
 * parameter, limited to the types below.
 * 
 * @author sam
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Configurable {
	public String name();

	public Type accepts();

	public enum Type {
		BooleanType() {
			public Object fromString(String s) {
				return Boolean.parseBoolean(s);
			}
		},
		IntType() {
			public Object fromString(String s) {
				return Integer.parseInt(s);
			}
		},
		LongType() {
			public Object fromString(String s) {
				return Long.parseLong(s);
			}
		},
		DoubleType() {
			public Object fromString(String s) {
				return Double.parseDouble(s);
			}
		},
		FloatType() {
			public Object fromString(String s) {
				return Float.parseFloat(s);
			}
		},
		StringType() {
			public Object fromString(String s) {
				return s;
			}
		};
		public abstract Object fromString(String value);
	}
}
