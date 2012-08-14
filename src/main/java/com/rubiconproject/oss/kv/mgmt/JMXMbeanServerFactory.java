package com.rubiconproject.oss.kv.mgmt;

import java.util.ArrayList;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

public class JMXMbeanServerFactory
{
	private JMXMbeanServerFactory()
	{
	}

	public static MBeanServer getMBeanServer()
	{
		MBeanServer mbserver = null;
		ArrayList<MBeanServer> mbservers = MBeanServerFactory.findMBeanServer(null);

		if (mbservers.size() > 0)
		{
			mbserver = (MBeanServer) mbservers.get(0);
		}

		if (mbserver != null)
		{
		}
		else
		{
			mbserver = MBeanServerFactory.createMBeanServer();
		}
		return mbserver;
	}

}
