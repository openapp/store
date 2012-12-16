package org.openapplication.store.leveldb.test;

import java.io.File;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.openapplication.store.Definition;
import org.openapplication.store.StoreClient;
import org.openapplication.store.leveldb.LevelDBStore;
import org.openapplication.store.test.StoreImplTest;

public class LevelDBStoreTest extends StoreImplTest {

	static LevelDBStore staticStore = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		File directory = new File("target/levelDbStore");
		directory.mkdir();
		staticStore = new LevelDBStore(directory, new Definition[] { PERSON,
				COMPANY, MEMBER, MEMBER_OF });
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		staticStore.close();
	}

	@Before
	public void setUp() throws Exception {
		store = new StoreClient(staticStore);
	}

	@After
	public void tearDown() throws Exception {
		store = null;
	}

}
