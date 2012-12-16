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
