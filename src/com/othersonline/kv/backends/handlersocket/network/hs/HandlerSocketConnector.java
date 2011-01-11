package com.othersonline.kv.backends.handlersocket.network.hs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

import com.othersonline.kv.backends.handlersocket.Command;
import com.othersonline.kv.backends.handlersocket.Protocol;
import com.othersonline.kv.backends.handlersocket.exception.HandlerSocketException;
import com.othersonline.kv.backends.handlersocket.impl.ReconnectRequest;
import com.othersonline.kv.backends.handlersocket.network.core.Session;

/**
 * Networking connector
 * 
 * @author dennis
 * @date 2010-11-29
 */
public interface HandlerSocketConnector {
	public boolean isStarted();

	public void setHealSessionInterval(long healConnectionInterval);

	public long getHealSessionInterval();

	public Protocol getProtocol();

	public void addSession(Session session);

	public void removeSession(Session session);

	public void addToWatingQueue(ReconnectRequest request);

	public Future<Boolean> connect(InetSocketAddress remoteAddr)
			throws IOException;

	public CopyOnWriteArrayList<Session> getSessionList();

	public void send(final Command msg) throws HandlerSocketException;

	public boolean isAllowAutoReconnect();

	public void setAllowAutoReconnect(boolean allowAutoReconnect);

}