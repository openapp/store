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
