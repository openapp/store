package org.openapplication.store.hbase;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.openapplication.store.Definition;
import org.openapplication.store.Entries;
import org.openapplication.store.Entry;

public final class HBaseIterable implements Entries {

	private final Iterable<Result> source;

	private final byte[] fromKey;

	private final byte[] toKey;

	private final List<Definition> definitions;

	public HBaseIterable(Iterable<Result> source, byte[] fromKey, byte[] toKey,
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
		final Iterator<Result> results = source.iterator();
		return new Iterator<Entry>() {
			@Override
			public boolean hasNext() {
				return results.hasNext();
			}

			@Override
			public Entry next() {
				Result keyValue = results.next();

				byte[] value;
				for (Definition definition : definitions)
					if (definition.matchesValue(value = keyValue.value()))
						return new Entry(new Entry(keyValue.getRow(),
								definition.getKeyFields()), new Entry(value,
								definition.getValueFields()));

				return new Entry(keyValue.getRow(), definitions.get(0)
						.getKeyFields());
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
