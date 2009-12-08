package com.othersonline.kv.test.backends;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import com.othersonline.kv.backends.TokyoCabinetKeyValueStore;
import com.othersonline.kv.test.KeyValueStoreBackendTestCase;
import com.othersonline.kv.transcoder.DoubleTranscoder;
import com.othersonline.kv.transcoder.IntegerTranscoder;
import com.othersonline.kv.transcoder.Transcoder;

public class TokyoCabinetBackendTestCase extends KeyValueStoreBackendTestCase {
	private String[] keys = new String[] { "abc", "def", "ghi", "abc" };

	private String[] values = new String[] { "entry # 1", "entry # 2",
			"entry # 3", "entry # 4" };

	private Transcoder integerTranscoder = new IntegerTranscoder();

	private Transcoder doubleTranscoder = new DoubleTranscoder();

	public void testBackend() throws Exception {
		doTestBTreeBackend();
		doTestHashBackend();
	}

	private void doTestBTreeBackend() throws Exception {
		File tempFile = File.createTempFile("test-btree", ".tcb");
		tempFile.deleteOnExit();
		TokyoCabinetKeyValueStore store = new TokyoCabinetKeyValueStore(
				tempFile.getCanonicalPath());
		store.setBtree(true);
		doTestTCBackend(store);
	}

	private void doTestHashBackend() throws Exception {
		File tempFile = File.createTempFile("test-hash", ".tch");
		tempFile.deleteOnExit();
		TokyoCabinetKeyValueStore store = new TokyoCabinetKeyValueStore(
				tempFile.getCanonicalPath());
		doTestTCBackend(store);
	}

	private void doTestTCBackend(TokyoCabinetKeyValueStore store)
			throws Exception {
		doTestBackend(store);

		// set some values
		for (int i = 0; i < keys.length; ++i) {
			store.set(keys[i], values[i]);
			assertEquals(store.get(keys[i]), values[i]);
		}

		// test iterator
		int count = 0;
		Iterator<String> keyIterator = store.iterkeys().iterator();
		while (keyIterator.hasNext()) {
			String key = keyIterator.next();
			assertNotNull(key);
			assertNotNull(store.get(key));
			++count;
		}
		assertEquals(count, 3);

		// test fwmkeys()
		List<String> fwmkeys = store.fwmkeys("", 10);
		assertEquals(fwmkeys.size(), 3);
		assertEquals(fwmkeys.get(0), keys[0]);
		fwmkeys = store.fwmkeys("", 10);
		assertEquals(fwmkeys.size(), 3);

		// test addint()
		String incrementKey = "someincrement";
		store.addint(incrementKey, 1);
		assertEquals(store.get(incrementKey, integerTranscoder), 1);
		store.addint(incrementKey, 142313);
		assertEquals(store.get(incrementKey, integerTranscoder), 142314);
		store.delete(incrementKey);

		// test adddouble()
		store.adddouble(incrementKey, 1231.231);
		assertEquals(store.get(incrementKey, doubleTranscoder), 1231.231);
		store.adddouble(incrementKey, 1023.12111);
		assertEquals(store.get(incrementKey, doubleTranscoder),
				1231.231 + 1023.12111);
		store.delete(incrementKey);

		// test rnum()
		long rnum = store.rnum();
		assertEquals(rnum, 3);

		// test fsiz()
		long fsiz = store.fsiz();
		assertTrue(fsiz > 1);

		// test optimize()
		assertTrue(store.optimize());

		// test sync()
		store.sync();

		// test vanish()
		assertTrue(store.vanish());
		assertEquals(store.rnum(), 0);

		// clean up
		for (int i = 0; i < keys.length; ++i) {
			store.delete(keys[i]);
		}
	}

}
