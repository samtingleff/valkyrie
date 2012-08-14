package com.rubiconproject.oss.kv;

import java.io.IOException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.rubiconproject.oss.kv.mgmt.BaseKeyValueStoreImplMXBean;
import com.rubiconproject.oss.kv.mgmt.JMXMbeanServerFactory;

public abstract class BaseManagedKeyValueStore extends BaseKeyValueStore
		implements ManagedKeyValueStore {
	public BaseManagedKeyValueStore() {
		initJMXBean();
	}

	public void finalize() throws Throwable {
		destroyJMXBean();
		super.finalize();
	}

	public void start() throws IOException {
		super.start();
	}

	public void stop() {
		super.stop();
	}

	public String getMXBeanObjectName() {
		return String
				.format(
						"OthersOnline:entity=KeyValueStore,class=%1$s,identifier=%2$s,id=%3$d",
						getClass().getName(), getIdentifier(), System
								.identityHashCode(this));
	}

	public Object getMXBean() {
		return new BaseKeyValueStoreImplMXBean(this);
	}

	protected void initJMXBean() {
		Object mxbean = getMXBean();
		MBeanServer mbeanServer = JMXMbeanServerFactory.getMBeanServer();
		String name = getMXBeanObjectName();
		try {
			ObjectName objectName = new ObjectName(name);
			mbeanServer.registerMBean(mxbean, objectName);
		} catch (InstanceAlreadyExistsException e) {
			try {
				ObjectName objectName = new ObjectName(name);
				mbeanServer.unregisterMBean(objectName);
				mbeanServer.registerMBean(mxbean, objectName);
			} catch (Exception e1) {
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void destroyJMXBean() {
		MBeanServer mbeanServer = JMXMbeanServerFactory.getMBeanServer();
		String name = getMXBeanObjectName();
		try {
			ObjectName objectName = new ObjectName(name);
			mbeanServer.unregisterMBean(objectName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
