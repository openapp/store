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

import java.util.Iterator;
import java.util.List;

import org.openapplication.store.ByteArrayComparator;
import org.openapplication.store.Definition;
import org.openapplication.store.Entries;
import org.openapplication.store.Entry;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public final class BerkeleyDBJEIterable implements Entries {

	private final Cursor source;

	private final DatabaseEntry key;

	private final DatabaseEntry value;

	private final byte[] fromKey;

	private final byte[] toKey;

	private final List<Definition> definitions;

	public BerkeleyDBJEIterable(Cursor source, DatabaseEntry key,
			DatabaseEntry value, byte[] fromKey, byte[] toKey,
			List<Definition> definitions) {
		this.source = source;
		this.key = key;
		this.value = value;
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

			private boolean hasNext = true;

			@Override
			public boolean hasNext() {
				if (!checked) {
					checked = true;
					if (!hasNext)
						hasNext = source.getNext(key, value,
								LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS;
					if (hasNext)
						hasNext = ByteArrayComparator.INSTANCE.compare(toKey,
								key.getData()) > 0;
					if (hasNext)
						return true;
					source.close();
				}
				return hasNext;
			}

			@Override
			public Entry next() {
				checked = false;
				hasNext = false;
				byte[] data;
				for (Definition definition : definitions)
					if (definition.matchesValue(data = value.getData()))
						return new Entry(new Entry(key.getData(),
								definition.getKeyFields()), new Entry(data,
								definition.getValueFields()));

				return new Entry(key.getData(), definitions.get(0)
						.getKeyFields());
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
