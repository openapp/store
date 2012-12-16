package org.openapplication.store.bdbje.test;

import java.io.File;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.openapplication.store.Definition;
import org.openapplication.store.StoreClient;
import org.openapplication.store.bdbje.BerkeleyDBJEStore;
import org.openapplication.store.test.StoreImplTest;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BerkeleyDBJEStoreTest extends StoreImplTest {

	static Environment myDbEnvironment = null;

	static BerkeleyDBJEStore staticStore = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		try {
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			File directory = new File("target/bdbjeEnv");
			if (!directory.exists())
				directory.mkdirs();
			myDbEnvironment = new Environment(directory, envConfig);

			staticStore = new BerkeleyDBJEStore(myDbEnvironment,
					new Definition[] { PERSON, COMPANY, MEMBER, MEMBER_OF });

		} catch (DatabaseException dbe) {
			dbe.printStackTrace();
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// TODO Properly close cursors
		// Closing the store currently results in error
		// "Database still has 2 open cursors while trying to close."
		// staticStore.close();
		// myDbEnvironment.close();
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
