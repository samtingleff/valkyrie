package com.othersonline.kv.distributed.backends;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.othersonline.kv.KeyValueStore;
import com.othersonline.kv.KeyValueStoreUnavailable;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.distributed.ConnectionFactory;

public abstract class AbstractConnectionFactory implements ConnectionFactory {
	private Map<String, KeyValueStore> backends = new HashMap<String, KeyValueStore>();

	public KeyValueStore getStore(String uri) throws IOException,
			KeyValueStoreUnavailable {
		KeyValueStore store = backends.get(uri);
		if (store == null) {
			synchronized (this) {
				if (store == null) {
					// yes i realize this is an anti-pattern. if we create an
					// extra connection here and there nobody is going to care.
					try {
						store = createStoreConnection(uri);
						store.start();
						backends.put(uri, store);
					} catch (IllegalArgumentException e) {
						throw new KeyValueStoreUnavailable(e);
					}
				}
			}
		}
		return store;
	}

	protected void configureStore(KeyValueStore store,
			Map<String, String> configs) throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {
		Method[] methods = store.getClass().getMethods();
		for (Method method : methods) {
			Configurable annotation = method.getAnnotation(Configurable.class);
			if (annotation != null) {
				String name = annotation.name();
				String value = configs.get(name);
				if (value != null) {
					Object obj = annotation.accepts().fromString(value);
					setFieldValue(store, method, obj);
				}
			}
		}
	}

	private void setFieldValue(KeyValueStore store, Method method, Object value)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		method.invoke(store, value);
	}

	protected abstract KeyValueStore createStoreConnection(String uri)
			throws IOException, KeyValueStoreUnavailable;

}
