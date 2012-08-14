package com.rubiconproject.oss.kv.backends;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.rubiconproject.oss.kv.KeyValueStore;
import com.rubiconproject.oss.kv.KeyValueStoreUnavailable;
import com.rubiconproject.oss.kv.annotations.Configurable;

public abstract class AbstractConnectionFactory implements ConnectionFactory {
	private Map<String, KeyValueStore> backends = new HashMap<String, KeyValueStore>();

	public KeyValueStore getStore(Map defaultProperties, String uri) throws IOException,
			KeyValueStoreUnavailable {
		KeyValueStore store = backends.get(uri);
		if (store == null) {
			synchronized (this) {
				// check again...
				store = backends.get(uri);
				if (store == null) {
					// yes i realize this is an anti-pattern. if we create an
					// extra connection here and there nobody is going to care.
					try {
						store = createStoreConnection(uri);
						// set the defaults from the provided map
						if (defaultProperties != null)
							configureStore(store, defaultProperties);
						// set values from the given url
						Map<String, String> props = getStoreProperties(uri);
						if (props != null)
							configureStore(store, props);
						
						store.start();
						backends.put(uri, store);
					} catch (IllegalArgumentException e) {
						throw new KeyValueStoreUnavailable(e);
					} catch (IllegalAccessException e) {
						throw new KeyValueStoreUnavailable(e);
					} catch (InvocationTargetException e) {
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

	public abstract Map<String, String> getStoreProperties(String uri)
		throws IllegalArgumentException;

	public abstract KeyValueStore createStoreConnection(String uri)
			throws IOException, KeyValueStoreUnavailable;

}
