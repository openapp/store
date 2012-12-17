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
import java.util.NoSuchElementException;

public interface Entries extends Iterable<Entry> {

	byte[] fromKey();

	byte[] toKey();

	Iterator<Entry> iterator();

	public static final Entries EMPTY = new Entries() {
		public final byte[] KEY = new byte[0];

		@Override
		public byte[] fromKey() {
			return KEY;
		}

		@Override
		public byte[] toKey() {
			return KEY;
		}

		@Override
		public Iterator<Entry> iterator() {
			return new Iterator<Entry>() {
				@Override
				public boolean hasNext() {
					return false;
				}

				@Override
				public Entry next() {
					throw new NoSuchElementException();
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}
	};

}
