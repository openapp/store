package org.openapplication.store.disk;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.List;

import org.openapplication.store.Blob;
import org.openapplication.store.Entries;
import org.openapplication.store.Entry;
import org.openapplication.store.Field;
import org.openapplication.store.MapKeyValue;
import org.openapplication.store.StoreServer;
import org.openapplication.store.StreamEncoding;

public final class DiskStore implements StoreServer {

	private final DiskMapProvider mapProvider;

	private final StoreServer storeImpl;

	public DiskStore(DiskMapProvider mapProvider, StoreServer storeImpl) {
		this.mapProvider = mapProvider;
		this.storeImpl = storeImpl;
	}

	@Override
	public List<MapKeyValue> put(Field<?>... fields) {
		List<MapKeyValue> ops = storeImpl.put(fields);

		for (MapKeyValue op : ops) {
			RandomAccessFile file = mapProvider.getFile(op.map, op.key);
			try {
				file.seek(file.length());

				file.writeShort(op.key.length);
				file.write(op.key);

				file.writeShort(op.value.length);
				file.write(op.value);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		return ops;
	}

	@Override
	public List<MapKeyValue> remove(Field<?>... fields) {
		List<MapKeyValue> ops = storeImpl.remove(fields);

		for (MapKeyValue op : ops) {
			RandomAccessFile file = mapProvider.getFile(op.map, op.key);
			try {
				file.seek(file.length());

				file.writeShort(0);

				file.writeShort(op.key.length);
				file.write(op.key);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		return ops;
	}

	@Override
	public Entry get(Field<?>... fields) {
		return storeImpl.get(fields);
	}

	@Override
	public Entries iterate(Entry first, Entry last, Field<?>... fields) {
		return storeImpl.iterate(first, last, fields);
	}

	@Override
	public Entries iterateNext(byte[] subsequent, Entry first, Entry last,
			Field<?>... fields) {
		return storeImpl.iterateNext(subsequent, first, last, fields);
	}

	@Override
	public InputStream read(Blob blob) {
		// TODO Implement persistence
		return storeImpl.read(blob);
	}

	@Override
	public Blob write(InputStream stream, StreamEncoding inEncoding,
			StreamEncoding outEncoding) {
		// TODO Implement persistence
		return storeImpl.write(stream, inEncoding, outEncoding);
	}

}
