package org.openapplication.store;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

public final class EntrySorter implements Iterable<Entry> {

	private final Iterable<Entry> source;

	private final Sort sort;

	private final int count;

	private final Entry after;

	public EntrySorter(Iterable<Entry> source, Sort sort) {
		this(source, sort, null, null);
	}

	public EntrySorter(Iterable<Entry> source, Sort sort, Integer count,
			Entry after) {
		this.source = source;
		this.sort = sort;
		this.count = count == null ? Integer.MAX_VALUE : count;
		this.after = after;
	}

	@Override
	public Iterator<Entry> iterator() {
		LinkedList<Entry> sorted = new LinkedList<Entry>();
		sort.sort(source.iterator(), count, after, sorted);
		return Collections.unmodifiableList(sorted).iterator();
	}

	public interface Sort {
		void sort(Iterator<Entry> source, int count, Entry after,
				LinkedList<Entry> sorted);
	}

	public static final class Unreverse<T extends Comparable<T>> implements
			Sort {
		private final Field<T> field;

		public Unreverse(Field<T> field) {
			this.field = field;
		}

		@Override
		public void sort(Iterator<Entry> source, int count, Entry after,
				LinkedList<Entry> sorted) {
			if (after == null)
				while (source.hasNext()) {
					sorted.addFirst(source.next());
					if (sorted.size() > count)
						sorted.removeLast();
				}
			else {
				T lastExclusive = after.get(field);
				while (source.hasNext()) {
					Entry next = source.next();
					if (0 <= lastExclusive.compareTo(next.get(field)))
						return;
					sorted.addFirst(next);
					if (sorted.size() > count)
						sorted.removeLast();
				}
			}
		}
	};

}
