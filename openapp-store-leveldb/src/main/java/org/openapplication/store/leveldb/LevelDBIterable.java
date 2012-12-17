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
