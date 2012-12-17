/**
 * Copyright 2012 Erik Isaksson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
