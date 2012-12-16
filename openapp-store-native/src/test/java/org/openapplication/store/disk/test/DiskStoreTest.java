package org.openapplication.store.disk.test;

import java.io.File;

import org.junit.Before;
import org.junit.BeforeClass;
import org.openapplication.store.Definition;
import org.openapplication.store.Store;
import org.openapplication.store.StoreClient;
import org.openapplication.store.StoreImpl;
import org.openapplication.store.disk.DiskMapProvider;
import org.openapplication.store.disk.DiskStore;
import org.openapplication.store.test.StoreImplTest;

public class DiskStoreTest extends StoreImplTest {

	private static Store diskStore;

	private static File testDir = new File("target/test-map-dir");

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		DiskMapProvider mapProvider = new DiskMapProvider(testDir);
		diskStore = new StoreClient(new DiskStore(mapProvider, new StoreImpl(
				mapProvider, new Definition[] { PERSON, COMPANY, MEMBER,
						MEMBER_OF })));

		diskStore.get(Id.PERSON.value(1));
		diskStore.get(Id.COMPANY.value(1));
		diskStore.get(Id.PERSON.value(1), Id.COMPANY.value(1));
		diskStore.get(Id.COMPANY.value(1), Id.PERSON.value(1));
	}

	@Before
	public void setUp() throws Exception {
		store = diskStore;
	}

}
