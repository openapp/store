package org.openapplication.store;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class EntryFilter implements Iterable<Entry> {

	private final Iterable<Entries> sources;

	private final Evaluate[] predicates;

	public EntryFilter(Iterable<Entries> sources, Evaluate... predicates) {
		this.sources = sources;
		this.predicates = predicates;
	}

	@Override
	public Iterator<Entry> iterator() {
		final Iterator<Entries> sourceIter = sources.iterator();
		return new Iterator<Entry>() {
			Iterator<Entry> entryIter = sourceIter.hasNext() ? sourceIter
					.next().iterator() : null;

			Entry nextEntry;

			boolean hasNext;

			@Override
			public boolean hasNext() {
				if (!hasNext)
					return checkNext();
				return true;
			}

			@Override
			public Entry next() {
				if (!hasNext && !checkNext())
					throw new NoSuchElementException();
				hasNext = false;
				return nextEntry;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			private boolean checkNext() {
				if (entryIter == null)
					entryIter = sourceIter.hasNext() ? sourceIter.next()
							.iterator() : null;
				while (entryIter != null) {
					while (entryIter != null && entryIter.hasNext())
						switch (evaluate(nextEntry = entryIter.next())) {
						case ACCEPT:
							return hasNext = true;
						case LAST_ACCEPT:
							entryIter = null;
							return hasNext = true;
						}
					entryIter = sourceIter.hasNext() ? sourceIter.next()
							.iterator() : null;
				}
				return false;
			}
		};
	}

	private Evaluation evaluate(Entry entry) {
		boolean rejectCurrent = false;
		boolean rejectRemaining = false;

		for (Evaluate predicate : predicates) {
			switch (predicate.evaluate(entry)) {
			case REJECT:
				rejectCurrent = true;
				break;
			case LAST_ACCEPT:
				rejectRemaining = true;
				break;
			case REJECT_ALL:
				rejectCurrent = true;
				rejectRemaining = true;
			}
			if (rejectCurrent && rejectRemaining)
				break;
		}

		if (rejectCurrent || rejectRemaining) {
			if (rejectCurrent && rejectRemaining)
				return Evaluation.REJECT_ALL;
			return rejectCurrent ? Evaluation.REJECT : Evaluation.LAST_ACCEPT;
		} else
			return Evaluation.ACCEPT;
	}

	public interface Evaluate {
		Evaluation evaluate(Entry entry);
	}

	public enum Evaluation {
		ACCEPT, REJECT, LAST_ACCEPT, REJECT_ALL
	}

	public static final class Values implements Evaluate {
		private final Field<?>[] fields;

		public Values(Field<?>... fields) {
			this.fields = fields;
		}

		@Override
		public Evaluation evaluate(Entry entry) {
			return entry.containsFieldValues(fields) ? Evaluation.ACCEPT
					: Evaluation.REJECT;
		}
	}

}
