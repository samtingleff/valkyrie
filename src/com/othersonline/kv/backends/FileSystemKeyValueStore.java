package com.othersonline.kv.backends;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import com.othersonline.kv.BaseManagedKeyValueStore;
import com.othersonline.kv.KeyValueStoreException;
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

	public void setRoot(String rootDirectory) {
		this.rootDirectory = rootDirectory;
	}

	public void setSanitizeKeys(boolean sanitizeKeys) {
		this.sanitizeKeys = sanitizeKeys;
	}

	public void setCleanEmptyDirectories(boolean removeEmptyDirectories) {
		this.removeEmptyDirectories = removeEmptyDirectories;
	}

	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void start() throws IOException {
		root = new File(rootDirectory);
		if (!root.exists()) {
			root.mkdirs();
		}
		assert (root.canRead());
		File tempFile = File.createTempFile("testfile", ".txt", root);
		tempFile.delete();
		super.start();
	}

	@Override
	public boolean exists(String key) throws KeyValueStoreException,
			IOException {
		assertReadable();
		File f = getFile(key);
		return f.exists();
	}

	@Override
	public Object get(String key) throws KeyValueStoreException, IOException,
			ClassNotFoundException {
		assertReadable();
		return get(key, defaultTranscoder);
	}

	@Override
	public Object get(String key, Transcoder transcoder)
			throws KeyValueStoreException, IOException, ClassNotFoundException {
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

	@Override
	public void set(String key, Serializable value)
			throws KeyValueStoreException, IOException {
		assertWriteable();
		set(key, value, defaultTranscoder);
	}

	@Override
	public void set(String key, Serializable value, Transcoder transcoder)
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

	@Override
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
