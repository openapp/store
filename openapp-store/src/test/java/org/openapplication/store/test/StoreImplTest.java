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
package org.openapplication.store.test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.openapplication.store.Blob;
import org.openapplication.store.Definition;
import org.openapplication.store.Entry;
import org.openapplication.store.Field;
import org.openapplication.store.FieldRange;
import org.openapplication.store.FieldValue;
import org.openapplication.store.Store;
import org.openapplication.store.StoreClient;
import org.openapplication.store.StoreImpl;
import org.openapplication.store.StreamEncoding;


public class StoreImplTest {

	public static Charset CHARSET = Charset.forName("UTF-8");

	public enum Id implements Field<Integer> {
		PERSON("3ee89670-8719-40da-b06b-9ea349b1de8d",
				"http://example.org/terms/person.id"),

		COMPANY("0792de01-da20-4898-8269-0d7559d397e9",
				"http://example.org/terms/company.id");

		private final UUID uuid;

		private final String uri;

		private Id(String uuid, String uri) {
			this.uuid = UUID.fromString(uuid);
			this.uri = uri;
		}

		@Override
		public Field<Integer> toField() {
			return this;
		}

		@Override
		public UUID toUuid() {
			return uuid;
		}

		@Override
		public String toUri() {
			return uri;
		}

		@Override
		public FieldValue<Integer> value(Integer value) {
			return new FieldValue<Integer>(this, value);
		}

		@Override
		public FieldRange<Integer> range(Integer min, Integer max) {
			return new FieldRange<Integer>(this, min, max);
		}

		@Override
		public int size() {
			return 4;
		}

		@Override
		public ByteBuffer toBytes(Object value) {
			return ByteBuffer.allocate(size()).putInt((Integer) value);
		}

		@Override
		public void put(ByteBuffer buffer, Object value) {
			buffer.putInt((Integer) value);
		}

		@Override
		public Integer get(ByteBuffer buffer) {
			return buffer.getInt();
		}

		@Override
		public String toString(Object value) {
			return value.toString();
		}

	}

	public enum Name implements Field<String> {
		PERSON("6ce400a9-bfca-4bf6-9179-918445d5a907",
				"http://example.org/terms/person.name"),

		COMPANY("7af7c287-a51a-4e0d-9f4b-5ef03b673b1a",
				"http://example.org/terms/company.name");

		private final UUID uuid;

		private final String uri;

		private Name(String uuid, String uri) {
			this.uuid = UUID.fromString(uuid);
			this.uri = uri;
		}

		@Override
		public Field<String> toField() {
			return this;
		}

		@Override
		public UUID toUuid() {
			return uuid;
		}

		@Override
		public String toUri() {
			return uri;
		}

		@Override
		public FieldValue<String> value(String value) {
			return new FieldValue<String>(this, value);
		}

		@Override
		public FieldRange<String> range(String min, String max) {
			return new FieldRange<String>(this, min, max);
		}

		@Override
		public int size() {
			return Field.SIZE_NULL_TERMINATED;
		}

		@Override
		public ByteBuffer toBytes(Object value) {
			return CHARSET.encode((String) value);
		}

		@Override
		public void put(ByteBuffer buffer, Object value) {
			buffer.put(CHARSET.encode((String) value));
		}

		@Override
		public String get(ByteBuffer buffer) {
			return CHARSET.decode(buffer).toString();
		}

		@Override
		public String toString(Object value) {
			return value.toString();
		}

	}

	public static Definition PERSON = new Definition(
			new Field<?>[] { Id.PERSON }, new Field<?>[] { Name.PERSON });

	public static Definition COMPANY = new Definition(
			new Field<?>[] { Id.COMPANY }, new Field<?>[] { Name.COMPANY });

	public static Definition MEMBER = new Definition(new Field<?>[] {
			Id.COMPANY, Id.PERSON }, new Field<?>[] {});

	public static Definition MEMBER_OF = new Definition(new Field<?>[] {
			Id.PERSON, Id.COMPANY }, new Field<?>[] {});

	protected Store store;

	@Before
	public void setUp() throws Exception {
		store = new StoreClient(new StoreImpl(new Definition[] { PERSON,
				COMPANY, MEMBER, MEMBER_OF }));
	}

	@Test
	public void testPut() {
		store.put(Id.PERSON.value(1), Name.PERSON.value("John"));
	}

	@Test
	public void testRemove() {
		store.put(Id.PERSON.value(1), Name.PERSON.value("John"));
		store.put(Id.COMPANY.value(1), Name.COMPANY.value("ACME"));

		store.remove(Id.PERSON.value(1));
		store.remove(Id.PERSON.value(2));

		Entry john = store.get(Id.PERSON.value(1));
		assertNull(john);

		Entry acme = store.get(Id.COMPANY.value(1));
		assertNotNull(acme);
	}

	@Test
	public void testGet() {
		store.put(Id.PERSON.value(2), Name.PERSON.value("Jane"));

		Entry jane = store.get(Id.PERSON.value(2));
		assertNotNull(jane);
		assertEquals("Jane", jane.get(Name.PERSON));
	}

	@Test
	public void testIterate() {
		store.put(Id.PERSON.value(1), Name.PERSON.value("John"));
		store.put(Id.PERSON.value(2), Name.PERSON.value("Jane"));

		store.put(Id.COMPANY.value(1), Name.COMPANY.value("Acme"));
		store.put(Id.COMPANY.value(2), Name.COMPANY.value("Cyberdyne"));

		store.put(Id.COMPANY.value(1), Id.PERSON.value(1));
		store.put(Id.COMPANY.value(2), Id.PERSON.value(2));

		Iterator<Entry> members = store.iterate(Id.COMPANY.value(1), Id.PERSON)
				.iterator();
		assertTrue(members.hasNext());
		Entry john = members.next();
		assertNotNull(john);
		assertEquals((Integer) 1, john.get(Id.PERSON));
		assertFalse(members.hasNext());

		members = store.iterate(Id.COMPANY.value(2), Id.PERSON).iterator();
		assertTrue(members.hasNext());
		Entry jane = members.next();
		assertNotNull(jane);
		assertEquals((Integer) 2, jane.get(Id.PERSON));
		assertFalse(members.hasNext());

		members = store.iterate(Id.COMPANY.value(3), Id.PERSON).iterator();
		assertFalse(members.hasNext());

		assertNull(store.iterate(Id.COMPANY, Id.COMPANY));
		assertNull(store.iterate(Id.PERSON, Id.PERSON));

		Iterator<Entry> memberOf = store
				.iterate(Id.PERSON.value(1), Id.COMPANY).iterator();
		assertTrue(memberOf.hasNext());
		john = memberOf.next();
		assertNotNull(john);
		assertEquals((Integer) 1, john.get(Id.PERSON));
		assertFalse(memberOf.hasNext());

		memberOf = store.iterate(Id.PERSON.value(2), Id.COMPANY).iterator();
		assertTrue(memberOf.hasNext());
		jane = memberOf.next();
		assertNotNull(jane);
		assertEquals((Integer) 2, jane.get(Id.PERSON));
		assertFalse(memberOf.hasNext());

		for (int i = 10; i < 100; i++)
			store.put(Id.PERSON.value(i), Name.PERSON.value("Person #" + i));

		int count = 0;
		for (Entry person : store.iterate(null, new Entry(Id.PERSON.value(50)),
				Id.PERSON)) {
			switch (count++) {
			case 0:
				assertEquals((Integer) 51, person.get(Id.PERSON));
				continue;
			case 1:
				assertEquals((Integer) 52, person.get(Id.PERSON));
			}
			break;
		}
		assertEquals(2, count);

		count = 0;
		for (Entry person : store.iterate(new Entry(Id.PERSON.value(500)),
				null, Id.PERSON)) {
			switch (count++) {
			case 0:
				assertEquals((Integer) 1, person.get(Id.PERSON));
				assertEquals("John", person.get(Name.PERSON));
				continue;
			case 1:
				assertEquals((Integer) 2, person.get(Id.PERSON));
				assertEquals("Jane", person.get(Name.PERSON));
				continue;
			case 2:
				assertEquals((Integer) 10, person.get(Id.PERSON));
			}
			break;
		}
		assertEquals(3, count);

		count = 0;
		for (Entry person : store.iterate(new Entry(Id.PERSON.value(92)),
				new Entry(Id.PERSON.value(94)), Id.PERSON)) {
			switch (count++) {
			case 0:
				assertEquals((Integer) 92, person.get(Id.PERSON));
				continue;
			case 1:
				assertEquals((Integer) 93, person.get(Id.PERSON));
				continue;
			case 2:
				assertEquals((Integer) 94, person.get(Id.PERSON));
				continue;
			}
			break;
		}
		assertEquals(3, count);
	}

	int testReadWriteCount = 0;
	java.security.SecureRandom rnd = new java.security.SecureRandom();

	@Test
	public void testReadWrite() throws IOException {
		byte[] helloWorld = "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!"
				.getBytes(CHARSET);
		// rnd.nextBytes(helloWorld);

		Blob blob = store.write(new ByteArrayInputStream(helloWorld),
				StreamEncoding.IDENTITY, StreamEncoding.IDENTITY);
		assertNotNull(blob);

		InputStream stream = store.read(blob);
		assertNotNull(stream);
		int read, i = 0;
		while ((read = stream.read()) != -1)
			assertEquals(helloWorld[i++] & 0xff, read);
		assertEquals(-1, read);
		assertEquals(helloWorld.length, i);

		// Test a few more times
		// if (testReadWriteCount == 0)
		// while (testReadWriteCount++ < 10)
		// testReadWrite();
	}
}
