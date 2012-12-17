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

import java.util.Iterator;

public final class EntryConverter<T> implements Iterable<T> {

	private final Iterable<Entry> source;

	private final Convert<T> convert;

	public EntryConverter(Iterable<Entry> source, Convert<T> convert) {
		this.source = source;
		this.convert = convert;
	}

	@Override
	public Iterator<T> iterator() {
		final Iterator<Entry> entryIter = source.iterator();
		return new Iterator<T>() {
			@Override
			public boolean hasNext() {
				return entryIter.hasNext();
			}

			@Override
			public T next() {
				return convert.convert(entryIter.next());
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	public interface Convert<T> {
		T convert(Entry entry);
	}

	public static final class Unbox<T> implements Convert<T> {
		private final Field<T> field;

		public Unbox(Field<T> field) {
			this.field = field;
		}

		@Override
		public T convert(Entry entry) {
			return entry.get(field);
		}
	}

}
