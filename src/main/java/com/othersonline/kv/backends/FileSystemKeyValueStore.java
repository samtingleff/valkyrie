package com.othersonline.kv.backends;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
import com.othersonline.kv.annotations.Configurable;
import com.othersonline.kv.annotations.Configurable.Type;
import com.othersonline.kv.transcoder.SerializableTranscoder;
import com.othersonline.kv.transcoder.Transcoder;
import com.othersonline.kv.util.StreamUtils;

/**
 * A key value store that uses the local file system for content.
 * 
 * @author samtingleff
 * 
 */
public class FileSystemKeyValueStore extends BaseManagedKeyValueStore {
	public static final String IDENTIFIER = "filesystem";

	private Transcoder defaultTranscoder = new SerializableTranscoder();

	private String rootDirectory;

	private File root;

	private boolean sanitizeKeys = true;

	private boolean removeEmptyDirectories = true;

	public FileSystemKeyValueStore() {
	}

	public FileSystemKeyValueStore(String rootDirectory) {
		this.rootDirectory = rootDirectory;
	}

	public FileSystemKeyValueStore(File root) {
		this.rootDirectory = root.getAbsolutePath();
	}

	@Configurable(name = "root", accepts = Type.StringType)
	public void setRoot(String rootDirectory) {
		this.rootDirectory = rootDirectory;
	}

	@Configurable(name = "sanitizeKeys", accepts = Type.BooleanType)
	public void setSanitizeKeys(boolean sanitizeKeys) {
		this.sanitizeKeys = sanitizeKeys;
	}

	@Configurable(name = "cleanEmptyDirectories", accepts = Type.BooleanType)
	public void setCleanEmptyDirectories(boolean removeEmptyDirectories) {
		this.removeEmptyDirectories = removeEmptyDirectories;
	}

	public String getIdentifier() {
		return IDENTIFIER;
	}

	public void start() throws IOException {
		root = new File(rootDirectory);
		if (!root.exists()) {
			root.mkdirs();
		}
		assert (root.canRead());
		super.start();
	}

	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		File f = getFile(key);
		return f.exists();
	}

	public Object get(String key) throws KeyValueStoreException, IOException {
		assertReadable();
		return get(key, defaultTranscoder);
	}

	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertReadable();
		File f = getFile(key);
		if (!f.exists())
			return null;
		try {
			byte[] bytes = StreamUtils.fileToBytes(f);
			Object obj = transcoder.decode(bytes);
			return obj;
		} finally {
		}
	}

	public Map<String, Object> getBulk(String... keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public Map<String, Object> getBulk(final List<String> keys)
			throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public Map<String, Object> getBulk(final List<String> keys,
			Transcoder transcoder) throws KeyValueStoreException, IOException {
		assertReadable();
		Map<String, Object> results = new HashMap<String, Object>();
		for (String key : keys) {
			Object obj = get(key, transcoder);
			if (obj != null)
				results.put(key, obj);
		}
		return results;
	}

	public void set(String key, Object value) throws KeyValueStoreException,
			IOException {
		assertWriteable();
		set(key, value, defaultTranscoder);
	}

	public void set(String key, Object value, Transcoder transcoder)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		File f = getFile(key);
		File tempFile = File.createTempFile("temp-file", ".tmp", f
				.getParentFile());
		try {
			OutputStream os = new FileOutputStream(tempFile);
			try {
				byte[] bytes = transcoder.encode(value);
				os.write(bytes);
			} finally {
				os.close();
			}
			tempFile.renameTo(f);
		} catch (Exception e) {
			try {
				tempFile.delete();
			} catch (Exception e1) {
			}
		}
	}

	public void delete(String key) throws KeyValueStoreException, IOException {
		assertWriteable();
		File f = getFile(key);
		if (f.exists()) {
			f.delete();
			if (removeEmptyDirectories) {
				// File parent = f.getParentFile();
				File current = f.getParentFile();
				while ((current.listFiles().length == 0)
						&& (!current.equals(root))) {
					current.delete();
					current = current.getParentFile();
				}
			}
		}
	}

	private File getFile(String key) throws IOException, KeyValueStoreException {
		if (sanitizeKeys)
			key = key.replaceAll("[^a-zA-Z0-9\\.\\-_\\+\\/]", "_");
		File file = new File(root, key);
		if (!(file.getCanonicalPath().startsWith(root.getCanonicalPath()))) {
			throw new KeyValueStoreException("permission denied");
		}
		File parent = file.getParentFile();
		if (!parent.exists())
			parent.mkdirs();
		return file;
	}
}
