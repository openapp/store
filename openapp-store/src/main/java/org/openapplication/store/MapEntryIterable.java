package org.openapplication.store;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class MapEntryIterable implements Entries {

	private final Iterable<Map.Entry<byte[], byte[]>> source;

	private final byte[] fromKey;

	private final byte[] toKey;

	private final List<Definition> definitions;

	public MapEntryIterable(Iterable<Map.Entry<byte[], byte[]>> source,
			byte[] fromKey, byte[] toKey, List<Definition> definitions) {
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
		final Iterator<Map.Entry<byte[], byte[]>> mapEntryIter = source
				.iterator();
		return new Iterator<Entry>() {
			@Override
			public boolean hasNext() {
				return mapEntryIter.hasNext();
			}

			@Override
			public Entry next() {
				Map.Entry<byte[], byte[]> keyValue = mapEntryIter.next();

				for (Definition definition : definitions)
					if (definition.matchesValue(keyValue.getValue()))
						return new Entry(new Entry(keyValue.getKey(),
								definition.getKeyFields()), new Entry(
								keyValue.getValue(),
								definition.getValueFields()));

				return new Entry(keyValue.getKey(), definitions.get(0)
						.getKeyFields());
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

}
