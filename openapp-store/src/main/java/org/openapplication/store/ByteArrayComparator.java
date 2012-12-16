package org.openapplication.store;

import java.util.Comparator;

public final class ByteArrayComparator implements Comparator<byte[]> {

	public static final ByteArrayComparator INSTANCE = new ByteArrayComparator();

	private ByteArrayComparator() {
	}

	@Override
	public int compare(byte[] o1, byte[] o2) {
		int o1Length = o1.length, o2Length = o2.length;
		int minLength = (o1Length <= o2Length) ? o1Length : o2Length;

		for (int i = 0, result; i < minLength; i++)
			if (0 != (result = (o1[i] & 0xff) - (o2[i] & 0xff)))
				return result;

		return o1Length - o2Length;
	}

}
