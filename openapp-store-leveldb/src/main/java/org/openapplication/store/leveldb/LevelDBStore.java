package org.openapplication.store.leveldb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
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

import static org.fusesource.leveldbjni.JniDBFactory.*;

public class LevelDBStore implements StoreServer, Closeable {

	private final List<Definition> definitionList;

	private final Map<UUID, List<Definition>> definitionMap;

	private final Map<UUID, DB> storageMap;

	private final StoreServer memoryStore;

	public LevelDBStore(File directory, Definition... definitions) {
		definitionList = new ArrayList<Definition>();
		definitionMap = new HashMap<UUID, List<Definition>>();
		storageMap = new HashMap<UUID, DB>();

		memoryStore = new StoreImpl();

		Options options = new Options();
		options.createIfMissing(true);
		options.cacheSize(1024 * 1024 * 100);
		options.compressionType(CompressionType.SNAPPY);
		Logger logger = new Logger() {
			public void log(String message) {
				System.out.println(message);
			}
		};
		options.logger(logger);

		pushMemoryPool(1024 * 512);

		System.out.println("Opening LevelDB databases");

		for (Definition definition : definitions) {
			UUID keyUuid = definition.toKeyUuid();

			definitionList.add(definition);

			if (!definitionMap.containsKey(keyUuid))
				definitionMap.put(keyUuid, new ArrayList<Definition>());
			definitionMap.get(keyUuid).add(definition);

			if (!storageMap.containsKey(keyUuid))
				try {
					DB db = factory.open(
							new File(directory, keyUuid.toString()), options);
					storageMap.put(keyUuid, db);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
		}

		System.out.println("Opening LevelDB databases done");
	}

	@Override
	public void close() throws IOException {
		for (DB db : storageMap.values())
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
			storageMap.get(operation.map).put(operation.key, operation.value);

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
			storageMap.get(operation.map).delete(operation.key);

		return ops;
	}

	@Override
	public Entry get(Field<?>... fields) {
		UUID keyUuid = Key.asUuid(fields);
		DB storage = storageMap.get(keyUuid);
		if (storage == null)
			return null;
		Entry keyEntry = new Entry(fields);

		byte[] value = storage.get(keyEntry.getBytes(fields));
		if (value == null)
			return null;
		for (Definition definition : definitionMap.get(keyUuid))
			if (definition.matchesValue(value))
				return new Entry(keyEntry, new Entry(value,
						definition.getValueFields()));

		return keyEntry;
	}

	@Override
	public Entries iterate(Entry first, Entry last, Field<?>... fields) {
		UUID keyUuid = Key.asUuid(fields);
		DB storage = storageMap.get(keyUuid);
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

		DBIterator iterator = storage.iterator();
		iterator.seek(startRow);

		return new LevelDBIterable(iterator, startRow, stopRow,
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
