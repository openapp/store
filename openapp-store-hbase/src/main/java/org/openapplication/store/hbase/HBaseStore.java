package org.openapplication.store.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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

public final class HBaseStore implements StoreServer, Closeable {

	private final List<Definition> definitionList;

	private final Map<UUID, List<Definition>> definitionMap;

	private final Map<UUID, HTableInterface> storageMap;

	private final StoreServer memoryStore;

	public HBaseStore(HTablePool hTablePool, HBaseAdmin hAdmin,
			Definition... definitions) {
		definitionList = new ArrayList<Definition>();
		definitionMap = new HashMap<UUID, List<Definition>>();
		storageMap = new HashMap<UUID, HTableInterface>();

		memoryStore = new StoreImpl();

		for (Definition definition : definitions) {
			UUID keyUuid = definition.toKeyUuid();

			definitionList.add(definition);

			if (!definitionMap.containsKey(keyUuid))
				definitionMap.put(keyUuid, new ArrayList<Definition>());
			definitionMap.get(keyUuid).add(definition);

			try {
				if (!hAdmin.tableExists(keyUuid.toString())) {
					HTableDescriptor hDescr = new HTableDescriptor(
							keyUuid.toString());
					hDescr.addFamily(new HColumnDescriptor(new byte[] { 'o' }));
					hAdmin.createTable(hDescr);
				}
				storageMap
						.put(keyUuid, hTablePool.getTable(keyUuid.toString()));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void close() throws IOException {
		for (HTableInterface hTable : storageMap.values())
			hTable.close();
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
			try {
				Put put = new Put(operation.key);
				put.add(new byte[] { 'o' }, new byte[] { 'a' }, operation.value);
				storageMap.get(operation.map).put(put);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

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
			try {
				Delete delete = new Delete(operation.key);
				storageMap.get(operation.map).delete(delete);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		return ops;
	}

	@Override
	public Entry get(Field<?>... fields) {
		UUID keyUuid = Key.asUuid(fields);
		HTableInterface storage = storageMap.get(keyUuid);
		if (storage == null)
			return null;
		Entry keyEntry = new Entry(fields);

		Get get = new Get(keyEntry.getBytes(fields));
		get.addColumn(new byte[] { 'o' }, new byte[] { 'a' });
		Result result;
		try {
			result = storage.get(get);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (result == null)
			return null;
		byte[] value = result.getValue(new byte[] { 'o' }, new byte[] { 'a' });
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
		HTableInterface storage = storageMap.get(keyUuid);
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

		Scan scan = new Scan(startRow, stopRow);
		scan.addColumn(new byte[] { 'o' }, new byte[] { 'a' });
		scan.setCaching(10);
		ResultScanner scanner;
		try {
			scanner = storage.getScanner(scan);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		List<Definition> definitions = definitionMap.get(keyUuid);

		return new HBaseIterable(scanner, startRow, stopRow, definitions);
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
