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
