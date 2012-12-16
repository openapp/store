package org.openapplication.store.leveldb;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.iq80.leveldb.DBIterator;
import org.openapplication.store.ByteArrayComparator;
import org.openapplication.store.Definition;
import org.openapplication.store.Entries;
import org.openapplication.store.Entry;


public final class LevelDBIterable implements Entries {

	private final DBIterator source;

	private final byte[] fromKey;

	private final byte[] toKey;

	private final List<Definition> definitions;

	public LevelDBIterable(DBIterator source, byte[] fromKey, byte[] toKey,
			List<Definition> definitions) {
		this.source = source;
		this.fromKey = fromKey;
		this.toKey = toKey;
		this.definitions = definitions;
	}

	@Override
	public byte[] fromKey() {
		return fromKey;
	}

	@Override
	public byte[] toKey() {
		return toKey;
	}

	@Override
	public Iterator<Entry> iterator() {
		return new Iterator<Entry>() {
			private boolean checked = false;

			private boolean hasNext = false;

			@Override
			public boolean hasNext() {
				if (!checked) {
					checked = true;
					if (hasNext = source.hasNext())
						hasNext = ByteArrayComparator.INSTANCE.compare(toKey,
								source.peekNext().getKey()) > 0;
					if (hasNext)
						return true;
					//source.close();
				}
				return hasNext;
			}

			@Override
			public Entry next() {
				checked = false;
				hasNext = false;
				Map.Entry<byte[], byte[]> next = source.next();
				byte[] value = next.getValue();
				for (Definition definition : definitions)
					if (definition.matchesValue(value))
						return new Entry(new Entry(next.getKey(),
								definition.getKeyFields()), new Entry(value,
								definition.getValueFields()));

				return new Entry(next.getKey(), definitions.get(0)
						.getKeyFields());
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
