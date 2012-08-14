package com.rubiconproject.oss.kv.server.main;

import java.io.File;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public abstract class BaseKVServerMain {

	protected static AbstractApplicationContext getContext(String[] paths) {
		AbstractApplicationContext ctx = null;
		String springConfig = System.getProperty("spring.config");
		if ((springConfig != null) && (springConfig.length() > 0)) {
			File file = new File(springConfig);
			if (file.exists() && file.canRead()) {
				ctx = new FileSystemXmlApplicationContext(springConfig);
			}
		}
		if (ctx == null) {
			ctx = new ClassPathXmlApplicationContext(paths);
		}
		return ctx;
	}
}
