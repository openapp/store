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
package org.openapplication.store;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

public class StoreClient implements Store {

	private final StoreServer server;

	public StoreClient(StoreServer server) {
		this.server = server;
	}

	@Override
	public final List<MapKeyValue> put(Field<?>... fields) {
		List<MapKeyValue> mapKeyValues = server.put(fields);

		// Entry entry = new Entry(fields);
		// System.out.println("{");
		// for (MapKeyValue mkv : mapKeyValues) {
		// System.out.println(" [");
		// for (Field<?> field : mkv.definition.getKeyFields()) {
		// System.out.print("  <" + field.toUri() + ">");
		// if (entry.get(field) != null)
		// System.out.print(" " + field.toString(entry.get(field)));
		// System.out.println();
		// }
		// System.out.println(" ]");
		// for (Field<?> field : mkv.definition.getValueFields()) {
		// System.out.print("  <" + field.toUri() + ">");
		// if (entry.get(field) != null)
		// System.out.print(" " + field.toString(entry.get(field)));
		// System.out.println();
		// }
		// System.out.println(" .");
		// }
		// System.out.println("}");

		return mapKeyValues;
	}

	@Override
	public final List<MapKeyValue> remove(Field<?>... fields) {
		return server.remove(fields);
	}

	@Override
	public final Entry get(Field<?>... fields) {
		return server.get(fields);
	}

	@Override
	public final Entries iterate(Field<?>... fields) {
		Entries entries = server.iterate(null, null, fields);
		return entries != null ? new EntryIteration(entries, null, null, fields)
				: null;
	}

	@Override
	public final Entries iterate(Entry first, Entry last, Field<?>... fields) {
		Entries entries = server.iterate(first, last, fields);
		return entries != null ? new EntryIteration(entries, first, last,
				fields) : null;
	}

	private final class EntryIteration implements Entries {

		private final Entries source;

		private final Entry first;

		private final Entry last;

		private final Field<?>[] fields;

		public EntryIteration(Entries source, Entry first, Entry last,
				Field<?>... fields) {
			this.source = source;
			this.first = first;
			this.last = last;
			this.fields = fields;
		}

		@Override
		public byte[] fromKey() {
			return first.getBytes(fields, false);
		}

		@Override
		public byte[] toKey() {
			return last.getBytes(fields, true);
		}

		@Override
		public Iterator<Entry> iterator() {
			return new Iterator<Entry>() {
				Entries currentSource = source;
				Iterator<Entry> entryIter = source.iterator();

				@Override
				public boolean hasNext() {
					if (entryIter.hasNext())
						return true;
					else
						while (currentSource != null
								&& (currentSource = server.iterateNext(
										currentSource.fromKey(), first, last,
										fields)) != null)
							if ((entryIter = currentSource.iterator())
									.hasNext())
								return true;
					return false;
				}

				@Override
				public Entry next() {
					return entryIter.next();
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}
	}

	@Override
	public final InputStream read(Blob blob) {
		return server.read(blob);
	}

	@Override
	public final Blob write(InputStream stream, StreamEncoding inEncoding,
			StreamEncoding outEncoding) {
		return server.write(stream, inEncoding, outEncoding);
	}

}
