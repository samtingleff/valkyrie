package com.rubiconproject.oss.kv.server.main;

import org.springframework.context.support.AbstractApplicationContext;

public class SimpleThriftServer extends BaseKVServerMain {
	private static String[] defaultServerSpringPaths = new String[] { "/com/othersonline/kv/server/applicationContext.xml" };

	public static void main(String[] args) {
		AbstractApplicationContext ctx = getContext(defaultServerSpringPaths);
		Runtime.getRuntime().addShutdownHook(new ShutdownThread(ctx));
		WaitLoop waitLoop = new WaitLoop();
		Thread t = new Thread(waitLoop);
		t.start();
	}

	private static class WaitLoop implements Runnable {
		public void run() {
			while (true) {
				try {
					Thread.sleep(100000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
				}
			}
		}
	}

	private static class ShutdownThread extends Thread {
		private AbstractApplicationContext ctx;

		public ShutdownThread(AbstractApplicationContext ctx) {
			this.ctx = ctx;
		}

		public void run() {
			ctx.registerShutdownHook();
		}
	}
}
