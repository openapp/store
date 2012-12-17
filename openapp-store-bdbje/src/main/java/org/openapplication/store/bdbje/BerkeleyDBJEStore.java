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
package org.openapplication.store.bdbje;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.openapplication.store.Blob;
import org.openapplication.store.Definition;
import org.openapplication.store.Entries;
import org.openapplication.store.Entry;
import org.openapplication.store.Field;
import org.openapplication.store.Key;
import org.openapplication.store.MapKeyValue;
import org.openapplication.store.StoreImpl;
import org.openapplication.store.StoreServer;
import org.openapplication.store.StreamEncoding;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class BerkeleyDBJEStore implements StoreServer, Closeable {

	private final List<Definition> definitionList;

	private final Map<UUID, List<Definition>> definitionMap;

	private final Environment environment;

	private final Map<UUID, Database> storageMap;

	private final StoreServer memoryStore;

	public BerkeleyDBJEStore(Environment environment, Definition... definitions) {
		definitionList = new ArrayList<Definition>();
		definitionMap = new HashMap<UUID, List<Definition>>();
		this.environment = environment;
		storageMap = new HashMap<UUID, Database>();

		memoryStore = new StoreImpl();

		for (Definition definition : definitions) {
			UUID keyUuid = definition.toKeyUuid();

			definitionList.add(definition);

			if (!definitionMap.containsKey(keyUuid))
				definitionMap.put(keyUuid, new ArrayList<Definition>());
			definitionMap.get(keyUuid).add(definition);

			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);

			Database db = environment.openDatabase(null, keyUuid.toString(),
					dbConfig);
			storageMap.put(keyUuid, db);
		}
	}

	@Override
	public void close() throws IOException {
		for (Database db : storageMap.values())
			db.close();
	}

	@Override
	public List<MapKeyValue> put(Field<?>... fields) {
		Entry entry = new Entry(fields);
		List<MapKeyValue> ops = new ArrayList<MapKeyValue>();

		for (Definition definition : definitionList)
			if (entry.containsFieldValues(definition.getKeyFields())
					&& entry.containsFieldValues(definition.getValueFields()))
				ops.add(new MapKeyValue(definition.toKeyUuid(), definition,
						entry.getBytes(definition.getKeyFields()), entry
								.getBytes(definition.getValueFields())));

		for (MapKeyValue operation : ops)
			storageMap.get(operation.map).put(null,
					new DatabaseEntry(operation.key),
					new DatabaseEntry(operation.value));

		environment.sync();

		return ops;
	}

	@Override
	public List<MapKeyValue> remove(Field<?>... fields) {
		Entry entry = new Entry(fields);
		List<MapKeyValue> ops = new ArrayList<MapKeyValue>();

		for (Definition definition : definitionList)
			if (entry.containsFields(definition.getKeyFields()))
				ops.add(new MapKeyValue(definition.toKeyUuid(), definition,
						entry.getBytes(definition.getKeyFields()), null));

		for (MapKeyValue operation : ops)
			storageMap.get(operation.map).delete(null,
					new DatabaseEntry(operation.key));

		environment.sync();

		return ops;
	}

	@Override
	public Entry get(Field<?>... fields) {
		UUID keyUuid = Key.asUuid(fields);
		Database storage = storageMap.get(keyUuid);
		if (storage == null)
			return null;
		Entry keyEntry = new Entry(fields);

		DatabaseEntry value = new DatabaseEntry();
		if (storage.get(null, new DatabaseEntry(keyEntry.getBytes(fields)),
				value, LockMode.READ_UNCOMMITTED) != OperationStatus.SUCCESS)
			return null;
		byte[] data = value.getData();
		for (Definition definition : definitionMap.get(keyUuid))
			if (definition.matchesValue(data))
				return new Entry(keyEntry, new Entry(data,
						definition.getValueFields()));

		return keyEntry;
	}

	@Override
	public Entries iterate(Entry first, Entry last, Field<?>... fields) {
		UUID keyUuid = Key.asUuid(fields);
		Database storage = storageMap.get(keyUuid);
		if (storage == null)
			return null;
		Entry entry = new Entry(fields);

		byte[] startRow;
		byte[] stopRow;
		if (first != null && last != null) {
			startRow = first.getBytes(fields, false);
			stopRow = last.getBytes(fields, true);
		} else if (first != null) {
			startRow = entry.getBytes(fields, false);
			stopRow = first.getBytes(fields, false);
		} else if (last != null) {
			startRow = last.getBytes(fields, true);
			stopRow = entry.getBytes(fields, true);
		} else {
			startRow = entry.getBytes(fields, false);
			stopRow = entry.getBytes(fields, true);
		}

		Cursor cursor = storage.openCursor(null, null);
		DatabaseEntry key = new DatabaseEntry(startRow);
		DatabaseEntry value = new DatabaseEntry();
		if (cursor.getSearchKeyRange(key, value, LockMode.READ_UNCOMMITTED) != OperationStatus.SUCCESS)
			return Entries.EMPTY;

		return new BerkeleyDBJEIterable(cursor, key, value, startRow, stopRow,
				definitionMap.get(keyUuid));
	}

	@Override
	public Entries iterateNext(byte[] subsequent, Entry first, Entry last,
			Field<?>... fields) {
		return null;
	}

	@Override
	public InputStream read(Blob blob) {
		// TODO Implement persistence
		return memoryStore.read(blob);
	}

	@Override
	public Blob write(InputStream stream, StreamEncoding inEncoding,
			StreamEncoding outEncoding) {
		// TODO Implement persistence
		return memoryStore.write(stream, inEncoding, outEncoding);
	}

}
