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
package org.openapplication.store.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.openapplication.store.Definition;
import org.openapplication.store.StoreClient;
import org.openapplication.store.hbase.HBaseStore;
import org.openapplication.store.test.StoreImplTest;

public class HBaseStoreIT extends StoreImplTest {

	static HBaseStore staticStore;

	static HTablePool hTablePool;

	static HBaseAdmin hAdmin;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Configuration hConf = HBaseConfiguration.create();
		hConf.addResource(new Path(
				"/home/erik/Software/hbase-0.94.1/conf/hbase-site.xml"));

		hTablePool = new HTablePool(hConf, 10);
		try {
			hAdmin = new HBaseAdmin(hConf);
		} catch (MasterNotRunningException e1) {
			throw new RuntimeException(e1);
		} catch (ZooKeeperConnectionException e1) {
			throw new RuntimeException(e1);
		}

		staticStore = new HBaseStore(hTablePool, hAdmin, new Definition[] {
				PERSON, COMPANY, MEMBER, MEMBER_OF });
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		staticStore.close();
		hTablePool.close();
		hAdmin.close();
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
